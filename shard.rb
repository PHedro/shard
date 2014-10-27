# encoding: utf-8
#!/usr/bin/env ruby
require 'json'
require 'time'
require 'fileutils'

#exemplo de input
#177.126.180.83 - - [15/Aug/2013:13:54:38 -0300] "GET /meme.jpg HTTP/1.1" 200 2148 "-" "userid=5352b590-05ac-11e3-9923-c3e7d8408f3a"

def initial_files_shard(server_index)
  config_file = File.read('config_shard.json')
  config_hash = JSON.parse(config_file)

  nodes = config_hash["nodes"]
  my_name = nodes[server_index.to_s]["name"]
  server_log_list = nodes[server_index.to_s]["files"]

  path = FileUtils.pwd
  server_log_list.each do |filepath|
    File.open(path + filepath, 'r') do |file|
      file.each_line do |line_input|
        shard_input_line my_name, line_input, nodes
      end
    end
  end
end

def shard_input_line(server_name, line_input, nodes)
  time_regex = /\[\d{2}\/\w{3}\/\d{4}\:\d{2}:\d{2}:\d{2}\s.{5}\]/
  uuid_regex = /\w{8}-\w{4}-\w{4}-\w{4}-\w{12}/

  log_time = line_input.match time_regex
  uuid = line_input.scan(uuid_regex)[0]
  server_to_go = uuid[0].ord % nodes.keys.length

  log_path = FileUtils.pwd
  timestamp_flat = log_time.to_s.gsub(/\W/, '')
  log_path += nodes[server_to_go.to_s]['path'] + '/tmp/usrs/' + uuid + '/'

  FileUtils.mkdir_p log_path
  log_partial_file = log_path + timestamp_flat + '_' + server_name

  File.open log_partial_file, 'a+' do |logfile|
    logfile.puts line_input
  end
end

def merge_log_file(server_index)
  config_file = File.read('config_shard.json')
  config_hash = JSON.parse(config_file)

  log_path = FileUtils.pwd
  partial_logs_path = log_path + config_hash["nodes"][server_index.to_s]["path"]
  partial_logs_path << '/tmp/'

  partial_directories = Dir[partial_logs_path + '/usrs/*/']

  partial_directories.each do |user_directory|
    final_log = partial_logs_path + File.basename(user_directory)
    File.open final_log, 'a+' do |log|
      Dir[user_directory + '*_*'].each do |timelog|
        File.open timelog, 'r' do |partial|
           partial.each do |partial_line|
             log.puts partial_line
           end
        end
      end
    end
  end
end

def delete_tmp_usrs_files(server_index)
  config_file = File.read('config_shard.json')
  config_hash = JSON.parse(config_file)

  log_path = FileUtils.pwd
  partial_logs_path = log_path + config_hash["nodes"][server_index.to_s]["path"]
  partial_logs_path << '/tmp/usrs/'

  FileUtils.rm_rf partial_logs_path
end

def thread_initial_files_shard(nodes)
  threads = []
  nodes.each do |node_index|
    threads << Thread.new { initial_files_shard node_index }
  end
  threads.each { |thr| thr.join }
end

def thread_merge_partial_logs(nodes)
  threads = []
  nodes.each do |node_index|
    threads << Thread.new { merge_log_file node_index }
  end
  threads.each { |thr| thr.join }
end

def thread_delete_tmp_files(nodes)
  threads = []
  nodes.each do |node_index|
    threads << Thread.new { delete_tmp_usrs_files node_index }
  end
  threads.each { |thr| thr.join }
end

def shard_logs
  config_file = File.read('config_shard.json')
  config_hash = JSON.parse(config_file)

  nodes = config_hash["nodes"].keys

  thread_initial_files_shard nodes
  thread_merge_partial_logs nodes
  thread_delete_tmp_files nodes

  '-= sharded! =-'
end