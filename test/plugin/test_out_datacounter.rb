require 'helper'

class DataCounterOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
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

  def create_driver(conf = CONFIG, tag='test.input')
    Fluent::Test::OutputTestDriver.new(Fluent::DataCounterOutput, tag).configure(conf)
  end

  def test_configure
    assert_raise(Fluent::ConfigError) {
      d = create_driver('')
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        count_key field
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        pattern1 hoge ^1\\d\\d$
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        count_key field
        pattern2 hoge ^1\\d\\d$
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
        count_key field
        pattern1 hoge ^1\\d\\d$
        pattern4 pos  ^4\\d\\d$
      ]
    }
    assert_raise(Fluent::ConfigError) {
      d = create_driver %[
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
    assert_equal :tag, d.instance.aggregate
    assert_equal 'datacount', d.instance.tag
    assert_nil d.instance.input_tag_remove_prefix
    assert_equal 'field', d.instance.count_key
    assert_equal 'ok ^2\d\d$', d.instance.pattern1

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
    ]
    assert_equal 30, d.instance.tick
  end

  def test_count_initialized
    d = create_driver %[
      aggregate all
      count_key field
      pattern1 hoge 1\d\d
      pattern2 moge 2\d\d
    ]
    assert_equal [0,0,0], d.instance.counts['all']
  end

  def test_countups
    d = create_driver
    assert_nil d.instance.counts['test.input']

    d.instance.countups('test.input', [0, 0, 0, 0, 0])
    assert_equal [0,0,0,0,0], d.instance.counts['test.input']
    d.instance.countups('test.input', [1, 1, 1, 0, 0])
    assert_equal [1,1,1,0,0], d.instance.counts['test.input']
    d.instance.countups('test.input', [0, 5, 1, 0, 0])
    assert_equal [1,6,2,0,0], d.instance.counts['test.input']
  end

  def test_stripped_tag
    d = create_driver
    assert_equal 'input', d.instance.stripped_tag('test.input')
    assert_equal 'test.input', d.instance.stripped_tag('test.test.input')
    assert_equal 'input', d.instance.stripped_tag('input')
  end

  def test_generate_output
    d = create_driver
    r1 = d.instance.generate_output({'test.input' => [60,240,120,180,0], 'test.input2' => [0,600,0,0,0]}, 60)
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

    d = create_driver %[
      aggregate all
      count_key field
      pattern1 hoge xxx\d\d
    ]
    r2 = d.instance.generate_output({'all' => [60,240]}, 60)
    assert_equal   60, r2['unmatched_count']
    assert_equal  1.0, r2['unmatched_rate']
    assert_equal 20.0, r2['unmatched_percentage']
    assert_equal  240, r2['hoge_count']
    assert_equal  4.0, r2['hoge_rate']
    assert_equal 80.0, r2['hoge_percentage']
  end

  def test_pattern_num
    assert_equal 20, Fluent::DataCounterOutput::PATTERN_MAX_NUM

    conf = %[
      aggregate all
      count_key field
    ]
    (1..20).each do |i|
      conf += "pattern#{i} name#{i} ^#{i}$\n"
    end
    d = create_driver(conf, 'test.max')
    d.run do
      (1..20).each do |j|
        d.emit({'field' => j})
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
    d1 = create_driver(CONFIG, 'test.tag1')
    d1.run do
      60.times do
        d1.emit({'target' => '200'})
        d1.emit({'target' => '100'})
        d1.emit({'target' => '200'})
        d1.emit({'target' => '400'})
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
    ], 'test.tag2')
    d2.run do
      60.times do
        d2.emit({'target' => '200'})
        d2.emit({'target' => '300 200'})
      end
    end
    d2.instance.flush_emit(120)
    emits = d2.emits
    assert_equal 1, emits.length
    data = emits[0]
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
  end

end
