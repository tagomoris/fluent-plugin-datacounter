class Fluent::DataCounterOutput < Fluent::Output
  Fluent::Plugin.register_output('datacounter', self)

  config_param :unit, :string, :default => 'minute'
  config_param :aggregate, :string, :default => 'tag'
  config_param :tag, :string, :default => 'datacount'
  config_param :input_tag_remove_prefix, :string, :default => nil
  config_param :count_key, :string

  # pattern0 reserved as unmatched counts
  config_param :pattern1, :string # string: NAME REGEXP
  (2..9).each do |i|
    config_param ('pattern' + i.to_s).to_sym, :string, :default => nil # NAME REGEXP
  end

  attr_accessor :counts
  attr_accessor :last_checked

  def configure(conf)
    super

    @unit = case @unit
            when 'minute' then :minute
            when 'hour' then :hour
            when 'day' then :day
            else
              raise Fluent::ConfigError, "flowcounter unit allows minute/hour/day"
            end
    @aggregate = case @aggregate
                 when 'tag' then :tag
                 when 'all' then :all
                 else
                   raise Fluent::ConfigError, "flowcounter aggregate allows tag/all"
                 end

    @patterns = [[0, 'unmatched', nil]]
    pattern_names = ['unmatched']
    (1..9).each do |i|
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

    if @input_tag_remove_prefix
      @removed_prefix_string = @input_tag_remove_prefix + '.'
      @removed_length = @removed_prefix_string.length
    end

    @counts = count_initialized
  end

  def start
    super
    @counts = count_initialized
    start_watch
  end

  def shutdown
    super
    @watcher.terminate
    @watcher.join
  end

  def count_initialized(keys=nil)
    # counts['tag'][num] = count
    if @aggregate == :all
      {'all' => ([0] * @patterns.length)}
    elsif keys
      values = Array.new(keys.length) {|i|
        Array.new(@patterns.length){|j| 0 }
      }
      Hash[[keys, values].transpose]
    else
      {}
    end
  end

  def countups(tag, counts)
    if @aggregate == :all
      tag = 'all'
    end
    @counts[tag] ||= [0] * @patterns.length

    counts.each_with_index do |count, i|
      @counts[tag][i] += count
    end
  end

  def stripped_tag(tag)
    return tag unless @input_tag_remove_prefix
    return tag[@removed_length..-1] if tag.start_with?(@removed_prefix_string) and tag.length > @removed_length
    return tag[@removed_length..-1] if tag == @input_tag_remove_prefix
    tag
  end

  def generate_output(counts, step)
    output = {}
    if @aggregate == :all
      sum = counts['all'].inject(:+)
      counts['all'].each_with_index do |count,i|
        name = @patterns[i][1]
        output[name + '_count'] = count
        output[name + '_rate'] = ((count * 100.0) / (1.00 * step)).floor / 100.0
        output[name + '_percentage'] = count * 100.0 / (1.00 * sum) if sum > 0
      end
      return output
    end

    counts.keys.each do |tag|
      t = stripped_tag(tag)
      sum = counts[tag].inject(:+)
      counts[tag].each_with_index do |count,i|
        name = @patterns[i][1]
        output[t + '_' + name + '_count'] = count
        output[t + '_' + name + '_rate'] = ((count * 100.0) / (1.00 * step)).floor / 100.0
        output[t + '_' + name + '_percentage'] = count * 100.0 / (1.00 * sum) if sum > 0
      end
    end
    output
  end

  def flush(step)
    flushed,@counts = @counts,count_initialized(@counts.keys.dup)
    generate_output(flushed, step)
  end

  def flush_emit(step)
    Fluent::Engine.emit(@tag, Fluent::Engine.now, flush(step))
  end

  def start_watch
    # for internal, or tests only
    @watcher = Thread.new(&method(:watch))
  end
  
  def watch
    # instance variable, and public accessable, for test
    @last_checked = Fluent::Engine.now
    tick = case @unit
           when :minute then 60
           when :hour then 3600
           when :day then 86400
           else 
             raise RuntimeError, "@unit must be one of minute/hour/day"
           end
    while true
      sleep 0.5
      if Fluent::Engine.now - @last_checked >= tick
        now = Fluent::Engine.now
        flush_emit(now - @last_checked)
        @last_checked = now
      end
    end
  end

  def emit(tag, es, chain)
    c = [0] * @patterns.length

    es.each do |time,record|
      value = record[@count_key]
      next if value.nil?

      value = value.to_s
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

    chain.next
  end
end
