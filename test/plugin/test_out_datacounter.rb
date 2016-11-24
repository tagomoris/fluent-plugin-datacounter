require 'helper'
require 'fluent/test/driver/output'

class DataCounterOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  def config_element(name = 'test', argument = '', params = {}, elements = [])
    Fluent::Config::Element.new(name, argument, params, elements)
  end

  CONFIG = %[
    unit minute
    aggregate tag
    input_tag_remove_prefix test
    count_key target
    pattern1 status2xx ^2\\d\\d$
    pattern2 status3xx ^3\\d\\d$
    pattern3 status4xx ^4\\d\\d$
    pattern4 status5xx ^5\\d\\d$
  ]

  CONFIG_OUTPUT_PER_TAG = %[
    unit minute
    aggregate tag
    output_per_tag yes
    tag_prefix d
    input_tag_remove_prefix test
    count_key target
    pattern1 status2xx ^2\\d\\d$
    pattern2 status3xx ^3\\d\\d$
    pattern3 status4xx ^4\\d\\d$
    pattern4 status5xx ^5\\d\\d$
    output_messages yes
  ]

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::DataCounterOutput).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      create_driver('')
    }
    assert_raise(Fluent::ConfigError) {
      create_driver %[
        count_key field
      ]
    }
    assert_raise(Fluent::ConfigError) {
      create_driver %[
        pattern1 hoge ^1\\d\\d$
      ]
    }
    assert_raise(Fluent::ConfigError) {
      create_driver %[
        count_key field
        pattern2 hoge ^1\\d\\d$
      ]
    }
    assert_raise(Fluent::ConfigError) {
      create_driver %[
        count_key field
        pattern1 hoge ^1\\d\\d$
        pattern4 pos  ^4\\d\\d$
      ]
    }
    assert_raise(Fluent::ConfigError) {
      create_driver %[
        count_key field
        pattern1 hoge ^1\\d\\d$
        pattern2 hoge ^4\\d\\d$
      ]
    }
    d = create_driver %[
      count_key field
      pattern1 ok ^2\\d\\d$
    ]
    assert_equal 60, d.instance.tick
    assert_equal "tag", d.instance.aggregate
    assert_equal 'datacount', d.instance.tag
    assert_nil d.instance.input_tag_remove_prefix
    assert_equal 'field', d.instance.count_key
    assert_equal 'ok ^2\d\d$', d.instance.pattern1
    assert_equal false, d.instance.outcast_unmatched

    d1 = create_driver %[
      unit minute
      count_key field
      pattern1 ok ^2\\d\\d$
    ]
    d2 = create_driver %[
      count_interval 60s
      count_key field
      pattern1 ok ^2\\d\\d$
    ]
    assert_equal d1.instance.tick, d2.instance.tick

    d = create_driver %[
      count_interval 5m
      count_key field
      pattern1 ok ^2\\d\\d$
    ]
    assert_equal 300, d.instance.tick

    d = create_driver %[
      count_interval 2h
      count_key field
      pattern1 ok ^2\\d\\d$
    ]
    assert_equal 7200, d.instance.tick

    d = create_driver %[
      count_interval 30s
      count_key field
      pattern1 ok ^2\\d\\d$
      outcast_unmatched yes
      output_messages yes
    ]
    assert_equal 30, d.instance.tick
    assert_equal true, d.instance.outcast_unmatched
    assert_equal true, d.instance.output_messages
  end

  def test_configure_output_per_tag
    d = create_driver(CONFIG_OUTPUT_PER_TAG)

    assert_equal true, d.instance.output_per_tag
    assert_equal 'd', d.instance.tag_prefix
    assert_equal true, d.instance.output_messages

    x_CONFIG_OUTPUT_PER_TAG_WITHOUT_TAG_PREFIX = %[
   unit minute
   aggregate tag
   output_per_tag yes
   input_tag_remove_prefix test
   count_key target
   pattern1 status2xx ^2\\d\\d$
   pattern2 status3xx ^3\\d\\d$
   pattern3 status4xx ^4\\d\\d$
   pattern4 status5xx ^5\\d\\d$
    ]
    assert_raise(Fluent::ConfigError) {
      d = create_driver(x_CONFIG_OUTPUT_PER_TAG_WITHOUT_TAG_PREFIX)
    }
  end

  def test_count_initialized
    d = create_driver %[
      aggregate all
      count_key field
      pattern1 hoge 1\d\d
      pattern2 moge 2\d\d
    ]
    assert_equal [0,0,0,0], d.instance.counts['all']
  end

  def test_countups
    d = create_driver
    assert_nil d.instance.counts['test.input']

    d.instance.countups('test.input', [0, 0, 0, 0, 0])
    assert_equal [0,0,0,0,0,0], d.instance.counts['test.input']
    d.instance.countups('test.input', [1, 1, 1, 0, 0])
    assert_equal [1,1,1,0,0,3], d.instance.counts['test.input']
    d.instance.countups('test.input', [0, 5, 1, 0, 0])
    assert_equal [1,6,2,0,0,9], d.instance.counts['test.input']
  end

  def test_stripped_tag
    d = create_driver
    assert_equal 'input', d.instance.stripped_tag('test.input')
    assert_equal 'test.input', d.instance.stripped_tag('test.test.input')
    assert_equal 'input', d.instance.stripped_tag('input')
  end

  def test_generate_output
    d = create_driver
    r1 = d.instance.generate_output({'test.input' => [60,240,120,180,0,600], 'test.input2' => [0,600,0,0,0,600]}, 60)
    assert_equal   60, r1['input_unmatched_count']
    assert_equal  1.0, r1['input_unmatched_rate']
    assert_equal 10.0, r1['input_unmatched_percentage']
    assert_equal  240, r1['input_status2xx_count']
    assert_equal  4.0, r1['input_status2xx_rate']
    assert_equal 40.0, r1['input_status2xx_percentage']
    assert_equal  120, r1['input_status3xx_count']
    assert_equal  2.0, r1['input_status3xx_rate']
    assert_equal 20.0, r1['input_status3xx_percentage']
    assert_equal  180, r1['input_status4xx_count']
    assert_equal  3.0, r1['input_status4xx_rate']
    assert_equal 30.0, r1['input_status4xx_percentage']
    assert_equal    0, r1['input_status5xx_count']
    assert_equal  0.0, r1['input_status5xx_rate']
    assert_equal  0.0, r1['input_status5xx_percentage']

    assert_equal    0, r1['input2_unmatched_count']
    assert_equal  0.0, r1['input2_unmatched_rate']
    assert_equal  0.0, r1['input2_unmatched_percentage']
    assert_equal  600, r1['input2_status2xx_count']
    assert_equal 10.0, r1['input2_status2xx_rate']
    assert_equal 100.0, r1['input2_status2xx_percentage']
    assert_equal    0, r1['input2_status3xx_count']
    assert_equal  0.0, r1['input2_status3xx_rate']
    assert_equal  0.0, r1['input2_status3xx_percentage']
    assert_equal    0, r1['input2_status4xx_count']
    assert_equal  0.0, r1['input2_status4xx_rate']
    assert_equal  0.0, r1['input2_status4xx_percentage']
    assert_equal    0, r1['input2_status5xx_count']
    assert_equal  0.0, r1['input2_status5xx_rate']
    assert_equal  0.0, r1['input2_status5xx_percentage']
    assert_nil r1['input_messages']
    assert_nil r1['input2_messages']

    d = create_driver (CONFIG + "\n output_messages true\n")
    r1 = d.instance.generate_output({'test.input' => [60,240,120,180,0,600], 'test.input2' => [0,600,0,0,0,600]}, 60)
    assert_equal  600, r1['input_messages']
    assert_equal  600, r1['input2_messages']

    d = create_driver %[
      aggregate all
      count_key field
      pattern1 hoge xxx\d\d
    ]
    r2 = d.instance.generate_output({'all' => [60,240,300]}, 60)
    assert_equal   60, r2['unmatched_count']
    assert_equal  1.0, r2['unmatched_rate']
    assert_equal 20.0, r2['unmatched_percentage']
    assert_equal  240, r2['hoge_count']
    assert_equal  4.0, r2['hoge_rate']
    assert_equal 80.0, r2['hoge_percentage']

    d = create_driver %[
      aggregate all
      count_key field
      pattern1 hoge xxx\d\d
      output_messages yes
    ]
    r2 = d.instance.generate_output({'all' => [60,240,300]}, 60)
    assert_equal   60, r2['unmatched_count']
    assert_equal  1.0, r2['unmatched_rate']
    assert_equal 20.0, r2['unmatched_percentage']
    assert_equal  240, r2['hoge_count']
    assert_equal  4.0, r2['hoge_rate']
    assert_equal 80.0, r2['hoge_percentage']
    assert_equal  300, r2['messages']
  end

  def test_generate_output_per_tag
    d = create_driver(CONFIG_OUTPUT_PER_TAG)
    result = d.instance.generate_output_per_tags({'test.input' => [60,240,120,180,0,600], 'test.input2' => [0,600,0,0,0,600]}, 60)
    assert_equal   60, result['input']['unmatched_count']
    assert_equal  1.0, result['input']['unmatched_rate']
    assert_equal 10.0, result['input']['unmatched_percentage']
    assert_equal  240, result['input']['status2xx_count']
    assert_equal  4.0, result['input']['status2xx_rate']
    assert_equal 40.0, result['input']['status2xx_percentage']
    assert_equal  120, result['input']['status3xx_count']
    assert_equal  2.0, result['input']['status3xx_rate']
    assert_equal 20.0, result['input']['status3xx_percentage']
    assert_equal  180, result['input']['status4xx_count']
    assert_equal  3.0, result['input']['status4xx_rate']
    assert_equal 30.0, result['input']['status4xx_percentage']
    assert_equal    0, result['input']['status5xx_count']
    assert_equal  0.0, result['input']['status5xx_rate']
    assert_equal  0.0, result['input']['status5xx_percentage']
    assert_equal  600, result['input']['messages']

    assert_equal    0, result['input2']['unmatched_count']
    assert_equal  0.0, result['input2']['unmatched_rate']
    assert_equal  0.0, result['input2']['unmatched_percentage']
    assert_equal  600, result['input2']['status2xx_count']
    assert_equal 10.0, result['input2']['status2xx_rate']
    assert_equal 100.0, result['input2']['status2xx_percentage']
    assert_equal    0, result['input2']['status3xx_count']
    assert_equal  0.0, result['input2']['status3xx_rate']
    assert_equal  0.0, result['input2']['status3xx_percentage']
    assert_equal    0, result['input2']['status4xx_count']
    assert_equal  0.0, result['input2']['status4xx_rate']
    assert_equal  0.0, result['input2']['status4xx_percentage']
    assert_equal    0, result['input2']['status5xx_count']
    assert_equal  0.0, result['input2']['status5xx_rate']
    assert_equal  0.0, result['input2']['status5xx_percentage']
    assert_equal  600, result['input2']['messages']

    d = create_driver %[
      aggregate all
      count_key field
      pattern1 hoge xxx\d\d
      output_per_tag yes
      tag_prefix d
    ]
    r = d.instance.generate_output_per_tags({'all' => [60,240,300]}, 60)
    assert_equal 1, r.keys.size
    assert_equal   60, r['all']['unmatched_count']
    assert_equal  1.0, r['all']['unmatched_rate']
    assert_equal 20.0, r['all']['unmatched_percentage']
    assert_equal  240, r['all']['hoge_count']
    assert_equal  4.0, r['all']['hoge_rate']
    assert_equal 80.0, r['all']['hoge_percentage']
  end

  def test_pattern_num
    assert_equal 20, Fluent::Plugin::DataCounterOutput::PATTERN_MAX_NUM

    conf = %[
      aggregate all
      count_key field
    ]
    (1..20).each do |i|
      conf += "pattern#{i} name#{i} ^#{i}$\n"
    end
    d = create_driver(conf)
    d.run(default_tag: 'test.max') do
      (1..20).each do |j|
        d.feed({'field' => j})
      end
    end
    r = d.instance.flush(60)
    assert_equal 1, r['name1_count']
    assert_equal 1, r['name2_count']
    assert_equal 1, r['name3_count']
    assert_equal 1, r['name4_count']
    assert_equal 1, r['name5_count']
    assert_equal 1, r['name6_count']
    assert_equal 1, r['name7_count']
    assert_equal 1, r['name8_count']
    assert_equal 1, r['name9_count']
    assert_equal 1, r['name10_count']
    assert_equal 1, r['name11_count']
    assert_equal 1, r['name12_count']
    assert_equal 1, r['name13_count']
    assert_equal 1, r['name14_count']
    assert_equal 1, r['name15_count']
    assert_equal 1, r['name16_count']
    assert_equal 1, r['name17_count']
    assert_equal 1, r['name18_count']
    assert_equal 1, r['name19_count']
    assert_equal 1, r['name20_count']
  end

  def test_emit
    d1 = create_driver(CONFIG)
    d1.run(default_tag: 'test.tag1') do
      60.times do
        d1.feed({'target' => '200'})
        d1.feed({'target' => '100'})
        d1.feed({'target' => '200'})
        d1.feed({'target' => '400'})
      end
    end
    r1 = d1.instance.flush(60)
    assert_equal 120, r1['tag1_status2xx_count']
    assert_equal 2.0, r1['tag1_status2xx_rate']
    assert_equal 50.0, r1['tag1_status2xx_percentage']

    assert_equal 60, r1['tag1_status4xx_count']
    assert_equal 1.0, r1['tag1_status4xx_rate']
    assert_equal 25.0, r1['tag1_status4xx_percentage']

    assert_equal 60, r1['tag1_unmatched_count']
    assert_equal 1.0, r1['tag1_unmatched_rate']
    assert_equal 25.0, r1['tag1_unmatched_percentage']

    assert_equal 0, r1['tag1_status3xx_count']
    assert_equal 0.0, r1['tag1_status3xx_rate']
    assert_equal 0.0, r1['tag1_status3xx_percentage']
    assert_equal 0, r1['tag1_status5xx_count']
    assert_equal 0.0, r1['tag1_status5xx_rate']
    assert_equal 0.0, r1['tag1_status5xx_percentage']

    d2 = create_driver(%[
      aggregate all
      count_key target
      pattern1 ok 2\\d\\d
      pattern2 redirect 3\\d\\d
      output_messages yes
    ])
    d2.run(default_tag: 'test.tag2') do
      60.times do
        d2.feed({'target' => '200'})
        d2.feed({'target' => '300 200'})
      end
      d2.instance.flush_emit(120)
    end
    events = d2.events
    assert_equal 1, events.length
    data = events[0]
    assert_equal 'datacount', data[0] # tag
    assert_equal 120, data[2]['ok_count']
    assert_equal 1.0, data[2]['ok_rate']
    assert_equal 100.0, data[2]['ok_percentage']
    assert_equal 0, data[2]['redirect_count']
    assert_equal 0.0, data[2]['redirect_rate']
    assert_equal 0.0, data[2]['redirect_percentage']
    assert_equal 0, data[2]['unmatched_count']
    assert_equal 0.0, data[2]['unmatched_rate']
    assert_equal 0.0, data[2]['unmatched_percentage']
    assert_equal 120, data[2]['messages']

    d3 = create_driver(%[
      count_key target
      input_tag_remove_prefix test
      pattern1 ok 2\\d\\d
      pattern2 redirect 3\\d\\d
      outcast_unmatched yes
    ])
    d3.run(default_tag: 'test.tag2') do
      60.times do
        d3.feed({'target' => '200'})
        d3.feed({'target' => '300'})
        d3.feed({'target' => '400'})
      end
      d3.instance.flush_emit(180)
    end
    events = d3.events
    assert_equal 1, events.length
    data = events[0]
    assert_equal 'datacount', data[0] # tag
    assert_equal 60, data[2]['tag2_unmatched_count']
    assert_nil data[2]['tag2_unmatched_percentage']
    assert_equal 60, data[2]['tag2_ok_count']
    assert_equal 50.0, data[2]['tag2_ok_percentage']
    assert_equal 60, data[2]['tag2_redirect_count']
    assert_equal 50.0, data[2]['tag2_redirect_percentage']

    d3 = create_driver(%[
      aggregate all
      count_key target
      pattern1 ok 2\\d\\d
      pattern2 redirect 3\\d\\d
      outcast_unmatched true
      output_messages true
    ])
    d3.run(default_tag: 'test.tag2') do
      60.times do
        d3.feed({'target' => '200'})
        d3.feed({'target' => '300'})
        d3.feed({'target' => '400'})
      end
      d3.instance.flush_emit(180)
    end
    events = d3.events
    assert_equal 1, events.length
    data = events[0]
    assert_equal 'datacount', data[0] # tag
    assert_equal 60, data[2]['unmatched_count']
    assert_nil data[2]['unmatched_percentage']
    assert_equal 60, data[2]['ok_count']
    assert_equal 50.0, data[2]['ok_percentage']
    assert_equal 60, data[2]['redirect_count']
    assert_equal 50.0, data[2]['redirect_percentage']
    assert_equal 180, data[2]['messages']
  end

  def test_emit_output_per_tag
    d1 = create_driver(CONFIG_OUTPUT_PER_TAG)
    d1.run(default_tag: 'test.tag1') do
      60.times do
        d1.feed({'target' => '200'})
        d1.feed({'target' => '100'})
        d1.feed({'target' => '200'})
        d1.feed({'target' => '400'})
      end
    end
    r1 = d1.instance.flush_per_tags(60)
    assert_equal 1, r1.keys.size
    r = r1['tag1']
    assert_equal 120, r['status2xx_count']
    assert_equal 2.0, r['status2xx_rate']
    assert_equal 50.0, r['status2xx_percentage']

    assert_equal 60, r['status4xx_count']
    assert_equal 1.0, r['status4xx_rate']
    assert_equal 25.0, r['status4xx_percentage']

    assert_equal 60, r['unmatched_count']
    assert_equal 1.0, r['unmatched_rate']
    assert_equal 25.0, r['unmatched_percentage']

    assert_equal 0, r['status3xx_count']
    assert_equal 0.0, r['status3xx_rate']
    assert_equal 0.0, r['status3xx_percentage']
    assert_equal 0, r['status5xx_count']
    assert_equal 0.0, r['status5xx_rate']
    assert_equal 0.0, r['status5xx_percentage']

    assert_equal 240, r['messages']

    d2 = create_driver(%[
      aggregate all
      count_key target
      pattern1 ok 2\\d\\d
      pattern2 redirect 3\\d\\d
      output_per_tag yes
      tag_prefix d
    ])
    d2.run(default_tag: 'test.tag2') do
      60.times do
        d2.feed({'target' => '200'})
        d2.feed({'target' => '300 200'})
      end
      d2.instance.flush_emit(120)
    end
    events = d2.events
    assert_equal 1, events.length
    data = events[0]
    assert_equal 'd.all', data[0] # tag
    assert_equal 120, data[2]['ok_count']
    assert_equal 1.0, data[2]['ok_rate']
    assert_equal 100.0, data[2]['ok_percentage']
    assert_equal 0, data[2]['redirect_count']
    assert_equal 0.0, data[2]['redirect_rate']
    assert_equal 0.0, data[2]['redirect_percentage']
    assert_equal 0, data[2]['unmatched_count']
    assert_equal 0.0, data[2]['unmatched_rate']
    assert_equal 0.0, data[2]['unmatched_percentage']

    d3 = create_driver(%[
      count_key target
      input_tag_remove_prefix test
      pattern1 ok 2\\d\\d
      pattern2 redirect 3\\d\\d
      outcast_unmatched yes
      output_per_tag yes
      tag_prefix d
    ])
    d3.run(default_tag: 'test.tag2') do
      60.times do
        d3.feed({'target' => '200'})
        d3.feed({'target' => '300'})
        d3.feed({'target' => '400'})
      end
      d3.instance.flush_emit(180)
    end
    events = d3.events
    assert_equal 1, events.length
    data = events[0]
    assert_equal 'd.tag2', data[0] # tag
    assert_equal 60, data[2]['unmatched_count']
    assert_nil data[2]['unmatched_percentage']
    assert_equal 60, data[2]['ok_count']
    assert_equal 50.0, data[2]['ok_percentage']
    assert_equal 60, data[2]['redirect_count']
    assert_equal 50.0, data[2]['redirect_percentage']

    d3 = create_driver(%[
      aggregate all
      count_key target
      pattern1 ok 2\\d\\d
      pattern2 redirect 3\\d\\d
      outcast_unmatched true
      output_per_tag yes
      tag_prefix ddd
    ])
    d3.run(default_tag: 'test.tag2') do
      60.times do
        d3.feed({'target' => '200'})
        d3.feed({'target' => '300'})
        d3.feed({'target' => '400'})
      end
      d3.instance.flush_emit(180)
    end
    events = d3.events
    assert_equal 1, events.length
    data = events[0]
    assert_equal 'ddd.all', data[0] # tag
    assert_equal 60, data[2]['unmatched_count']
    assert_nil data[2]['unmatched_percentage']
    assert_equal 60, data[2]['ok_count']
    assert_equal 50.0, data[2]['ok_percentage']
    assert_equal 60, data[2]['redirect_count']
    assert_equal 50.0, data[2]['redirect_percentage']
  end

  def test_zero_tags
    fields = ['unmatched','status2xx','status3xx','status4xx','status5xx'].map{|k| 'tag1_' + k}.map{|p|
      ['count', 'rate', 'percentage'].map{|a| p + '_' + a}
    }.flatten
    fields_without_percentage = ['unmatched','status2xx','status3xx','status4xx','status5xx'].map{|k| 'tag1_' + k}.map{|p|
      ['count', 'rate'].map{|a| p + '_' + a}
    }.flatten

    d = create_driver(CONFIG)
    # CONFIG = %[
    #   unit minute
    #   aggregate tag
    #   input_tag_remove_prefix test
    #   count_key target
    #   pattern1 status2xx ^2\\d\\d$
    #   pattern2 status3xx ^3\\d\\d$
    #   pattern3 status4xx ^4\\d\\d$
    #   pattern4 status5xx ^5\\d\\d$
    # ]
    d.run(default_tag: 'test.tag1') do
      60.times do
        d.feed({'target' => '200'})
        d.feed({'target' => '100'})
        d.feed({'target' => '200'})
        d.feed({'target' => '400'})
      end
      d.instance.flush_emit(60)
      assert_equal 1, d.events.size
      r1 = d.events[0][2]
      assert_equal fields, r1.keys

      d.instance.flush_emit(60)
      assert_equal 2, d.events.size # +1
      r2 = d.events[1][2]
      assert_equal fields_without_percentage, r2.keys
      assert_equal [0]*10, r2.values

      d.instance.flush_emit(60)
      assert_equal 2, d.events.size # +0
    end
  end
  def test_zer_tags_per_tag
    fields = (['unmatched','status2xx','status3xx','status4xx','status5xx'].map{|p|
      ['count', 'rate', 'percentage'].map{|a| p + '_' + a}
    }.flatten + ['messages']).sort
    fields_without_percentage = (['unmatched','status2xx','status3xx','status4xx','status5xx'].map{|p|
      ['count', 'rate'].map{|a| p + '_' + a}
    }.flatten + ['messages']).sort

    d = create_driver(CONFIG_OUTPUT_PER_TAG)
    # CONFIG_OUTPUT_PER_TAG = %[
    #   unit minute
    #   aggregate tag
    #   output_per_tag yes
    #   tag_prefix d
    #   input_tag_remove_prefix test
    #   count_key target
    #   pattern1 status2xx ^2\\d\\d$
    #   pattern2 status3xx ^3\\d\\d$
    #   pattern3 status4xx ^4\\d\\d$
    #   pattern4 status5xx ^5\\d\\d$
    #   output_messages yes
    # ]
    d.run(default_tag: 'test.tag1') do
      60.times do
        d.feed({'target' => '200'})
        d.feed({'target' => '100'})
        d.feed({'target' => '200'})
        d.feed({'target' => '400'})
      end
      d.instance.flush_emit(60)
      assert_equal 1, d.events.size
      r1 = d.events[0][2]
      assert_equal fields, r1.keys.sort

      d.instance.flush_emit(60)
      assert_equal 2, d.events.size # +1
      r2 = d.events[1][2]
      assert_equal fields_without_percentage, r2.keys.sort
      assert_equal [0]*11, r2.values # (_count, _rate)x5 + messages

      d.instance.flush_emit(60)
      assert_equal 2, d.events.size # +0
    end
  end

  def test_store_file
    dir = "test/tmp"
    Dir.mkdir dir unless Dir.exist? dir
    file = "#{dir}/test.dat"
    File.unlink file if File.exist? file

    config = {
      "unit" =>  "minute",
      "aggregate" => "tag",
      "input_tag_remove_prefix" => "test",
      "count_key" =>  " target",
      "pattern1" => "status2xx ^2\\d\\d$",
      "pattern2" => "status3xx ^3\\d\\d$",
      "pattern3" => "status4xx ^4\\d\\d$",
      "pattern4" => "status5xx ^5\\d\\d$",
      "store_storage" => true
    }
    conf = config_element('ROOT', '', config, [
                            config_element(
                              'storage', '',
                              {'@type' => 'local',
                               '@id' => 'test-01',
                               'path' => "#{file}",
                               'persistent' => true,
                               })
                           ])
    # test store
    d = create_driver(conf)
    time = Fluent::Engine.now
    d.run(default_tag: 'test.input') do
      d.instance.flush_emit(60)
      d.feed(time, {'target' => 1})
      d.feed(time, {'target' => 1})
      d.feed(time, {'target' => 1})
    end
    stored_counts = d.instance.counts
    stored_saved_at = d.instance.saved_at
    stored_saved_duration = d.instance.saved_duration
    assert File.exist? file

    # test load
    d = create_driver(conf)
    loaded_counts = 0
    loaded_saved_at = 0
    loaded_saved_duration = 0
    d.run(default_tag: 'test.input') do
      loaded_counts = d.instance.counts
      loaded_saved_at = d.instance.saved_at
      loaded_saved_duration = d.instance.saved_duration
    end
    assert_equal stored_counts, loaded_counts
    assert_equal stored_saved_at, loaded_saved_at
    assert_equal stored_saved_duration, loaded_saved_duration

    # test not to load if config is changed
    d = create_driver(conf.merge("count_key" => "foobar", "store_storage" => true))
    d.run(default_tag: 'test.input') do
      loaded_counts = d.instance.counts
      loaded_saved_at = d.instance.saved_at
      loaded_saved_duration = d.instance.saved_duration
    end
    assert_equal({}, loaded_counts)
    assert_equal(nil, loaded_saved_at)
    assert_equal(nil, loaded_saved_duration)

    # test not to load if stored data is outdated.
    Delorean.jump 61 # jump more than count_interval
    d = create_driver(conf.merge("store_storage" => true))
    d.run(default_tag: 'test.input') do
      loaded_counts = d.instance.counts
      loaded_saved_at = d.instance.saved_at
      loaded_saved_duration = d.instance.saved_duration
    end
    assert_equal({}, loaded_counts)
    assert_equal(nil, loaded_saved_at)
    assert_equal(nil, loaded_saved_duration)
    Delorean.back_to_the_present

    File.unlink file
  end
end
