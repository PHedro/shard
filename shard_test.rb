require 'test/unit'
require 'fileutils'
require './shard'

class ShardTest < Test::Unit::TestCase

  def setup
    @line_input =  '177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=0352b590-05ac-11e3-9923-c3e7d8408f3a"'
    @server_name = 'Server1'
    @server_index = 0
    @nodes = {
        '0' => {
        'name' => "Server1",
        'path' => "/server_1",
        'files' => ["/server_1/tmp/sample_file_more"]
      }
    }
    @nodes_paths = ["/server_1", "/server_2", "/server_3", "/server_4"]
    @path = FileUtils.pwd
    @node_shard = Shard.new 0
  end

  def teardown
    @nodes_paths.each do |path_node|
      tmps_path = "#{@path}#{path_node}/tmp/"
      partial_logs_path = "#{tmps_path}usrs/"
      FileUtils.rm_rf partial_logs_path
      Dir["#{tmps_path}*"].each do |file|
        File.delete file unless file == "#{tmps_path}sample_file_more"
      end
    end
  end

  def test_shard_input_line
    file_expected = @path + '/server_1/tmp/usrs/0352b590-05ac-11e3-9923-c3e7d8408f3a/15Aug20131354380300_Server1'
    @node_shard.shard_input_line @line_input
    result = File.file?(file_expected)
    assert_equal(true, result)
  end

  def test_delete_specific
    file_expected = @path + '/server_1/tmp/usrs/0352b590-05ac-11e3-9923-c3e7d8408f3a/15Aug20131354380300_Server1'
    @node_shard.shard_input_line @line_input
    @node_shard.delete_tmp_user_specific_files '0352b590-05ac-11e3-9923-c3e7d8408f3a'
    result = File.file?(file_expected)
    assert_equal(false, result)
  end

  def test_shard_input_line_correct
    file_expected = @path + '/server_1/tmp/usrs/0352b590-05ac-11e3-9923-c3e7d8408f3a/15Aug20131354380300_Server1'
    @node_shard.shard_input_line @line_input
    result = ''
    File.open file_expected, 'r' do |f|
      result = f.readline
    end
    assert_equal(@line_input + "\n", result)
  end

  def test_merge_correct
    @node_shard.shard_input_line @line_input
    file_expected = @path + '/server_1/tmp/0352b590-05ac-11e3-9923-c3e7d8408f3a'
    @node_shard.merge_log_file
    result = File.file?(file_expected)
    assert_equal(true, result)
  end

  def test_delete_temp_files
    expected = @path + '/server_1/tmp/usrs'
    @node_shard.shard_input_line @line_input
    @node_shard.merge_log_file
    @node_shard.delete_tmp_usrs_files
    result = File.directory?(expected)
    assert_equal(false, result)
  end

  def test_shard_logs
    file_expected_one = @path + '/server_1/tmp/0352b590-05ac-11e3-9923-c3e7d8408f3a'
    file_expected_two = @path + '/server_2/tmp/a352b590-05ac-11e3-9923-c3e7d8408f3a'
    file_expected_three = @path + '/server_3/tmp/f352b590-05ac-11e3-9923-c3e7d8408f33'
    file_expected_four = @path + '/server_4/tmp/c85f124a-05cd-11e3-8a11-a8206608c527'

    nodes = [@node_shard, Shard.new(1), Shard.new(2), Shard.new(3)]
    Shard.shard_logs nodes

    result_one = File.file?(file_expected_one)
    result_two = File.file?(file_expected_two)
    result_three = File.file?(file_expected_three)
    result_four = File.file?(file_expected_four)
    assert_equal(true, result_one)
    assert_equal(true, result_two)
    assert_equal(true, result_three)
    assert_equal(true, result_four)
  end
end