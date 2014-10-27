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
    @nodes_four = {
        '0' => {
        'name' => "Server1",
        'path' => "/server_1",
        'files' => ["/server_1/tmp/sample_file_more"]
      },
        '1' => {
        'name' => "Server2",
        'path' => "/server_2",
        'files' => ["/server_2/tmp/sample_file_more"]
      },
        '2' => {
        'name' => "Server3",
        'path' => "/server_3",
        'files' => ["/server_3/tmp/sample_file_more"]
      },
        '3' => {
        'name' => "Server4",
        'path' => "/server_4",
        'files' => ["/server_4/tmp/sample_file_more"]
      }
    }
    @nodes_paths = ["/server_1", "/server_2", "/server_3", "/server_4"]
    @path = FileUtils.pwd
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
    shard_input_line @server_name, @line_input, @nodes
    result = File.file?(file_expected)
    assert_equal(true, result)
  end

  def test_shard_input_line_correct
    file_expected = @path + '/server_1/tmp/usrs/0352b590-05ac-11e3-9923-c3e7d8408f3a/15Aug20131354380300_Server1'
    shard_input_line @server_name, @line_input, @nodes
    result = ''
    File.open file_expected, 'r' do |f|
      result = f.readline
    end
    assert_equal(@line_input + "\n", result)
  end

  def test_merge_correct
    shard_input_line @server_name, @line_input, @nodes
    file_expected = @path + '/server_1/tmp/0352b590-05ac-11e3-9923-c3e7d8408f3a'
    merge_log_file @server_index
    result = File.file?(file_expected)
    assert_equal(true, result)
  end

  def test_delete_temp_files
    expected = @path + '/server_1/tmp/usrs'
    shard_input_line @server_name, @line_input, @nodes
    merge_log_file @server_index
    delete_tmp_usrs_files @server_index
    result = File.directory?(expected)
    assert_equal(false, result)
  end

  def test_shard_logs
    file_expected = @path + '/server_1/tmp/0352b590-05ac-11e3-9923-c3e7d8408f3a'
    shard_logs
    result = File.file?(file_expected)
    assert_equal(true, result)
  end

  def test_thr_init_shard_one
    file_expected_one = @path + '/server_1/tmp/usrs/0352b590-05ac-11e3-9923-c3e7d8408f34/15Aug20131401480300_Server3'
    thread_initial_files_shard @nodes_four.keys
    result_one = File.file?(file_expected_one)
    assert_equal(true, result_one)
  end

  def test_thr_init_shard_two
    file_expected_two = @path + '/server_2/tmp/usrs/5352b590-05ac-11e3-9923-c3e7d8408f3a/15Aug20131354380300_Server2'
    thread_initial_files_shard @nodes_four.keys
    result_two = File.file?(file_expected_two)
    assert_equal(true, result_two)
  end

  def test_thr_init_shard_three
    file_expected_three = @path + '/server_3/tmp/usrs/f85f124a-05cd-11e3-8a11-a8206608c529/15Aug20131354380300_Server1'
    thread_initial_files_shard @nodes_four.keys
    result_three = File.file?(file_expected_three)
    assert_equal(true, result_three)
  end

  def test_thr_init_shard_four
    file_expected_four = @path + '/server_4/tmp/usrs/c85f124a-05cd-11e3-8a11-a8206608c527/15Aug20131358380300_Server3'
    thread_initial_files_shard @nodes_four.keys
    result_four = File.file?(file_expected_four)
    assert_equal(true, result_four)
  end

  def test_merge_partial_logs_one
    file_expected_one = @path + '/server_1/tmp/0352b590-05ac-11e3-9923-c3e7d8408f3a'
    thread_initial_files_shard @nodes_four.keys
    thread_merge_partial_logs @nodes_four.keys
    result_one = File.file?(file_expected_one)
    assert_equal(true, result_one)
  end

  def test_merge_partial_logs_two
    file_expected_two = @path + '/server_2/tmp/a352b590-05ac-11e3-9923-c3e7d8408f3a'
    thread_initial_files_shard @nodes_four.keys
    thread_merge_partial_logs @nodes_four.keys
    result_two = File.file?(file_expected_two)
    assert_equal(true, result_two)
  end

  def test_merge_partial_logs_three
    file_expected_three = @path + '/server_3/tmp/f352b590-05ac-11e3-9923-c3e7d8408f33'
    thread_initial_files_shard @nodes_four.keys
    thread_merge_partial_logs @nodes_four.keys
    result_three = File.file?(file_expected_three)
    assert_equal(true, result_three)
  end

  def test_merge_partial_logs_four
    file_expected_four = @path + '/server_4/tmp/c85f124a-05cd-11e3-8a11-a8206608c527'
    thread_initial_files_shard @nodes_four.keys
    thread_merge_partial_logs @nodes_four.keys
    result_four = File.file?(file_expected_four)
    assert_equal(true, result_four)
  end
end