require 'pathname'
require 'fluent/plugin/output'

class Fluent::Plugin::DataCounterOutput < Fluent::Plugin::Output
  Fluent::Plugin.register_output('datacounter', self)

  helpers :event_emitter, :storage, :timer

  DEFAULT_STORAGE_TYPE = 'local'
  PATTERN_MAX_NUM = 20

  config_param :count_interval, :time, default: nil,
               desc: 'The interval time to count in seconds.'
  config_param :unit, :string, default: 'minute',
               desc: 'The interval time to monitor specified an unit (either of minute, hour, or day).'
  config_param :output_per_tag, :bool, default: false,
               desc: 'Emit for each input tag. tag_prefix must be specified together.'
  config_param :aggregate, :string, default: 'tag',
               desc: 'Calculate in each input tag separetely, or all records in a mass.'
  config_param :tag, :string, default: 'datacount',
               desc: 'The output tag.'
  config_param :tag_prefix, :string, default: nil,
               desc: 'The prefix string which will be added to the input tag.'
  config_param :input_tag_remove_prefix, :string, default: nil,
               desc: 'The prefix string which will be removed from the input tag.'
  config_param :count_key, :string,
               desc: 'The key to count in the event record.'
  config_param :outcast_unmatched, :bool, default: false,
               desc: 'Specify yes if you do not want to include \'unmatched\' counts into percentage. '
  config_param :output_messages, :bool, default: false,
               desc: 'Specify yes if you want to get tested messages.'
  config_param :store_file, :string, default: nil,
               obsoleted: 'Use store_storage parameter instead.',
               desc: 'Store internal data into a file of the given path on shutdown, and load on starting.'
  config_param :store_storage, :bool, default: false,
               desc: 'Store internal data into a storage on shutdown, and load on starting.'

  # pattern0 reserved as unmatched counts
  config_param :pattern1, :string, # string: NAME REGEXP
               desc: 'Specify RegexpName and Regexp. format: NAME REGEXP'
  (2..PATTERN_MAX_NUM).each do |i|
    config_param ('pattern' + i.to_s).to_sym, :string, default: nil, # NAME REGEXP
                 desc: "Specify RegexpName and Regexp. format: NAME REGEXP"
  end

  attr_accessor :tick
  attr_accessor :counts
  attr_accessor :saved_duration
  attr_accessor :saved_at
  attr_accessor :last_checked

  def configure(conf)
    super

    if @count_interval
      @tick = @count_interval.to_i
    else
      @tick = case @unit
              when 'minute' then 60
              when 'hour' then 3600
              when 'day' then 86400
              else
                raise RuntimeError, "@unit must be one of minute/hour/day"
              end
    end

    @aggregate = case @aggregate
                 when 'tag' then "tag"
                 when 'all' then "all"
                 else
                   raise Fluent::ConfigError, "datacounter aggregate allows tag/all"
                 end

    @patterns = [[0, 'unmatched', nil]]
    pattern_names = ['unmatched']

    pattern_keys = conf.keys.select{|k| k =~ /^pattern(\d+)$/}
    invalids = pattern_keys.select{|arg| arg =~ /^pattern(\d+)/ and not (1..PATTERN_MAX_NUM).include?($1.to_i)}
    if invalids.size > 0
      log.warn "invalid number patterns (valid pattern number:1-20):" + invalids.join(",")
    end
    (1..PATTERN_MAX_NUM).each do |i|
      next unless conf["pattern#{i}"]
      name,regexp = conf["pattern#{i}"].split(' ', 2)
      @patterns.push([i, name, Regexp.new(regexp)])
      pattern_names.push(name)
    end
    pattern_index_list = conf.keys.select{|s| s =~ /^pattern\d$/}.map{|v| (/^pattern(\d)$/.match(v))[1].to_i}
    unless pattern_index_list.reduce(true){|v,i| v and @patterns[i]}
      raise Fluent::ConfigError, "jump of pattern index found"
    end
    unless @patterns.length == pattern_names.uniq.length
      raise Fluent::ConfigError, "duplicated pattern names"
    end

    if @output_per_tag
      raise Fluent::ConfigError, "tag_prefix must be specified with output_per_tag" unless @tag_prefix
      @tag_prefix_string = @tag_prefix + '.'
    end

    if @input_tag_remove_prefix
      @removed_prefix_string = @input_tag_remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end

    if @store_storage
      config = conf.elements.select{|e| e.name == 'storage'}.first
      @storage = storage_create(usage: 'out_datacounter_store', conf: config,
                                default_type: DEFAULT_STORAGE_TYPE)
    end

    @counts = count_initialized
    @mutex = Mutex.new
  end

  def start
    super
    load_status(@tick) if @store_storage
    start_watch
  end

  def shutdown
    super
    save_status() if @store_storage
  end

  def count_initialized(keys=nil)
    # counts['tag'][num] = count
    # counts['tag'][-1] = sum
    if @aggregate == "all"
      {'all' => ([0] * (@patterns.length + 1))}
    elsif keys
      values = Array.new(keys.length) {|i|
        Array.new(@patterns.length + 1){|j| 0 }
      }
      Hash[[keys, values].transpose]
    else
      {}
    end
  end

  def countups(tag, counts)
    if @aggregate == "all"
      tag = 'all'
    end

    @mutex.synchronize {
      @counts[tag] ||= [0] * (@patterns.length + 1)
      sum = 0
      counts.each_with_index do |count, i|
        sum += count
        @counts[tag][i] += count
      end
      @counts[tag][-1] += sum
    }
  end

  def stripped_tag(tag)
    return tag unless @input_tag_remove_prefix
    return tag[@removed_length..-1] if tag.start_with?(@removed_prefix_string) and tag.length > @removed_length
    return tag[@removed_length..-1] if tag == @input_tag_remove_prefix
    tag
  end

  def generate_fields(step, target_counts, attr_prefix, output)
    sum = if @outcast_unmatched
            target_counts[1..-2].inject(:+)
          else
            target_counts[-1]
          end
    messages = target_counts.delete_at(-1)

    target_counts.each_with_index do |count,i|
      name = @patterns[i][1]
      output[attr_prefix + name + '_count'] = count
      output[attr_prefix + name + '_rate'] = ((count * 100.0) / (1.00 * step)).floor / 100.0
      unless i == 0 and @outcast_unmatched
        output[attr_prefix + name + '_percentage'] = count * 100.0 / (1.00 * sum) if sum > 0
      end
      if @output_messages
        output[attr_prefix + 'messages'] = messages
      end
    end

    output
  end

  def generate_output(counts, step)
    if @aggregate == "all"
      return generate_fields(step, counts['all'], '', {})
    end

    output = {}
    counts.keys.each do |tag|
      generate_fields(step, counts[tag], stripped_tag(tag) + '_', output)
    end
    output
  end

  def generate_output_per_tags(counts, step)
    if @aggregate == "all"
      return {'all' => generate_fields(step, counts['all'], '', {})}
    end

    output_pairs = {}
    counts.keys.each do |tag|
      output_pairs[stripped_tag(tag)] = generate_fields(step, counts[tag], '', {})
    end
    output_pairs
  end

  def flush(step) # returns one message
    flushed = nil
    @mutex.synchronize {
      flushed,@counts = @counts,count_initialized(@counts.keys.dup.select{|k| @counts[k][-1] > 0})
    }
    generate_output(flushed, step)
  end

  def flush_per_tags(step) # returns map of tag - message
    flushed = nil
    @mutex.synchronize {
      flushed,@counts = @counts,count_initialized(@counts.keys.dup.select{|k| @counts[k][-1] > 0})
    }
    generate_output_per_tags(flushed, step)
  end

  def flush_emit(step)
    if @output_per_tag
      # tag - message maps
      time = Fluent::Engine.now
      flush_per_tags(step).each do |tag,message|
        router.emit(@tag_prefix_string + tag, time, message)
      end
    else
      message = flush(step)
      if message.keys.size > 0
        router.emit(@tag, Fluent::Engine.now, message)
      end
    end
  end

  def start_watch
    # for internal, or tests only
    @last_checked ||= Fluent::Engine.now
    timer_execute(:out_datacounter_timer, 0.5, &method(:watch))
  end

  def watch
    # instance variable, and public accessable, for test
    if Fluent::Engine.now - @last_checked >= @tick
      now = Fluent::Engine.now
      flush_emit(now - @last_checked)
      @last_checked = now
    end
  end

  def process(tag, es)
    c = [0] * @patterns.length

    es.each do |time,record|
      value = record[@count_key]
      next if value.nil?

      value = value.to_s.force_encoding('ASCII-8BIT')
      matched = false
      @patterns.each do |index, name, regexp|
        next unless regexp and regexp.match(value)
        c[index] += 1
        matched = true
        break
      end
      c[0] += 1 unless matched
    end
    countups(tag, c)
  end

  # Store internal status into a storage
  #
  def save_status()
    begin
      @saved_at = Fluent::Engine.now
      @saved_duration = @saved_at - @last_checked
      patterns = @patterns.map{|idx, label, regexp|
        if regexp
          [idx, label, regexp.source]
        else
          [idx, label, regexp]
        end
      }
      value = {
        "counts"           => @counts,
        "saved_at"        => @saved_at,
        "saved_duration"  => @saved_duration,
        "aggregate"        => @aggregate,
        "count_key"        => @count_key,
        "patterns"         => patterns,
      }
      @storage.put(:stored_value, value)
    rescue => e
      log.warn "out_datacounter: Can't write store_storage #{e.class} #{e.message}"
    end
  end

  # Load internal status from a storage
  #
  # @param [Interger] tick The count interval
  def load_status(tick)
    return unless @storage.get(:stored_value)

    begin
      stored = @storage.get(:stored_value)
      patterns = stored["patterns"].map{|idx, label, regexp|
        if regexp
          [idx, label, Regexp.compile(regexp)]
        else
          [idx, label, regexp]
        end
      }
      if stored["aggregate"] == @aggregate and
        stored["count_key"] == @count_key and
        patterns == @patterns

        if Fluent::Engine.now <= stored["saved_at"] + tick
          @mutex.synchronize {
            @counts = stored["counts"]
            @saved_at = stored["saved_at"]
            @saved_duration = stored["saved_duration"]

            # skip the saved duration to continue counting
            @last_checked = Fluent::Engine.now - @saved_duration
          }
        else
          log.warn "out_datacounter: stored data is outdated. ignore stored data"
        end
      else
        log.warn "out_datacounter: configuration param was changed. ignore stored data"
      end
    rescue => e
      log.warn "out_datacounter: Can't load store_storage #{e.class} #{e.message}"
    end
  end

end
