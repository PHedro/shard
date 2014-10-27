# encoding: utf-8
#!/usr/bin/env ruby
require 'json'
require 'time'
require 'fileutils'

#exemplo de input
#177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"

class Shard
  def initialize(server_index)
    @server_index = server_index
    config_file = File.read('config_shard.json')
    @config_hash = config_hash = JSON.parse(config_file)

    @nodes = nodes = config_hash["nodes"]
    @my_name = nodes[server_index.to_s]["name"]
    @my_log_list = nodes[server_index.to_s]["files"]
    @my_rel_path = nodes[server_index.to_s]["path"]

    @my_abs_path = FileUtils.pwd
  end

  def initial_files_shard
    @my_log_list.each do |filepath|
      File.open(@my_abs_path + filepath, 'r') do |file|
        file.each_line do |line_input|
          self.shard_input_line line_input
        end
      end
    end
  end

  def shard_input_line(line_input)
    time_regex = /\[\d{2}\/\w{3}\/\d{4}\:\d{2}:\d{2}:\d{2}\s.{5}\]/
    uuid_regex = /\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/

    log_time = line_input.match time_regex
    uuid = line_input.scan(uuid_regex)[0]
    server_to_go = uuid[0].ord % @nodes.keys.length

    timestamp_flat = log_time.to_s.gsub(/\W/, '')
    log_dir = "#{@my_abs_path}#{@nodes[server_to_go.to_s]['path']}/tmp/usrs/#{uuid}/"

    FileUtils.mkdir_p log_dir
    log_partial_file = "#{log_dir}#{timestamp_flat}_#{@my_name}"

    File.open log_partial_file, 'a+' do |logfile|
      logfile.puts line_input
    end
  end


  def merge_log_file
    logs_path = "#{@my_abs_path}#{@my_rel_path}/tmp/"

    partial_directories = Dir["#{logs_path}/usrs/*/"]
    merge_threads = []
    partial_directories.each do |user_directory|
      merge_threads << Thread.new { merge_user(logs_path, user_directory) }
    end
    merge_threads.each { |thr| thr.join }
  end

  def merge_user(logs_path, user_directory)
    uuid = File.basename(user_directory)
    final_log = logs_path + uuid
    File.open final_log, 'a+' do |log|
      Dir[user_directory + '*_*'].each do |timelog|
        File.open timelog, 'r' do |partial|
          partial.each do |partial_line|
            log.puts partial_line
          end
        end
      end
    end
    delete_tmp_user_specific_files(uuid)
  end

  def delete_tmp_user_specific_files(uuid)
    FileUtils.rm_rf @my_abs_path + @my_rel_path + '/tmp/usrs/' + uuid + '/'
  end

  def delete_tmp_usrs_files
    FileUtils.rm_rf @my_abs_path + @my_rel_path + '/tmp/usrs/'
  end

  def self.shard_logs(nodes)
    threads = []
    nodes.each do |node|
      threads << Thread.new { node.initial_files_shard }
    end
    threads.each { |thr| thr.join }

    threads = []
    nodes.each do |node|
      threads << Thread.new {  node.merge_log_file }
    end
    threads.each { |thr| thr.join }

    threads = []
    nodes.each do |node|
      threads << Thread.new { node.delete_tmp_usrs_files }
    end
    threads.each { |thr| thr.join }
    '-= sharded! =-'
  end
end