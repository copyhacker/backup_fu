require 'yaml'
require 'active_support'
require 'mime/types'
require 'right_aws'
require 'erb'

class BackupFuConfigError < StandardError; end
class S3ConnectError < StandardError; end

class BackupFu
  
  def initialize
    db_conf = YAML.load_file(File.join(RAILS_ROOT, 'config', 'database.yml')) 
    @db_conf = db_conf[RAILS_ENV].symbolize_keys
    
    raw_config = File.read(File.join(RAILS_ROOT, 'config', 'backup_fu.yml'))
    erb_config = ERB.new(raw_config).result 
    fu_conf    = YAML.load(erb_config)
    @fu_conf   = fu_conf[RAILS_ENV].symbolize_keys
    
    @s3_conf = YAML.load_file(File.join(RAILS_ROOT, 'config', 'amazon_s3.yml'))[RAILS_ENV].symbolize_keys
    @fu_conf[:s3_bucket] ||= @s3_conf[:bucket_name]
    @fu_conf[:aws_access_key_id] ||= @s3_conf[:access_key_id]
    @fu_conf[:aws_secret_access_key] ||= @s3_conf[:secret_access_key]

    @fu_conf[:mysqldump_options] ||= '--complete-insert --skip-extended-insert'
    @verbose = !@fu_conf[:verbose].nil?
    @timestamp = datetime_formatted
    @fu_conf[:keep_backups] ||= 5
    check_conf
    create_dirs
  end
  
  def sqlcmd_options
    host, port, password = '', '', ''

    if @db_conf.has_key?(:host) && @db_conf[:host] != 'localhost'
      host = "--host=#{@db_conf[:host]}"
    end

    if @db_conf.has_key?(:port)
      port = "--port=#{@db_conf[:port]}"
    end

    unless @db_conf[:username].blank?
      user = "--user=#{@db_conf[:username]}"
    end

    if !@db_conf[:password].blank? && @db_conf[:adapter] != 'postgresql'
      password = "--password=#{@db_conf[:password]}"
    end

    "#{host} #{port} #{user} #{password}"
  end

  def pgpassword_prefix
    if !@db_conf[:password].blank? 
      "PGPASSWORD=#{@db_conf[:password]}"
    end
  end

  def dump
    full_dump_path = File.join(dump_base_path, db_filename)
    case @db_conf[:adapter]
    when 'postgresql'
      cmd = niceify "#{pgpassword_prefix} #{dump_path} -i -F c -b #{sqlcmd_options} #{@db_conf[:database]} > #{full_dump_path}"
    when 'mysql'
      cmd = niceify "#{dump_path} #{@fu_conf[:mysqldump_options]} #{sqlcmd_options} #{@db_conf[:database]} > #{full_dump_path}"
    end
    puts cmd if @verbose
    `#{cmd}`

    if !@fu_conf[:disable_compression]
      compress_db(dump_base_path, db_filename) 
      File.unlink full_dump_path
    end
  end

  def backup
    dump
    
    file = final_db_dump_path()
    puts "\nBacking up to S3: #{file}\n" if @verbose
    
    store_file(file)
  end
  
  def list_backups
    s3_connection.bucket(@fu_conf[:s3_bucket]).keys.map(&:to_s)
  end

  # Don't count on being able to drop the database, but do expect to drop all tables
  def prepare_db_for_restore
    raise "restore unimplemented for #{adapter}" unless (adapter = @db_conf[:adapter]) == 'postgresql'
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'"
    cmd = "psql #{@db_conf[:database]} -t -c \"#{query}\""
    puts "Executing: '#{cmd}'"
    tables = `#{cmd}`

    query = "DROP TABLE #{tables.map(&:chomp).map(&:strip).reject(&:empty?).join(", ")} CASCADE"
    cmd = "psql #{@db_conf[:database]} -t -c \"#{query}\""
    puts "Executing: '#{cmd}'"
    `#{cmd}`
  end

  def restore_backup(key)
    raise "Restore not implemented for #{@db_conf[:adapter]}" unless @db_conf[:adapter] == 'postgresql'
    raise 'Restore not implemented for zip' if @fu_conf[:compressor] == 'zip'
    
    restore_file_name = @fu_conf[:disable_compression] ? 'restore.sql' : 'restore.tar.gz'
    restore_file = Tempfile.new(restore_file_name)
    
    open(restore_file.path, 'w') do |fh|
      puts "Fetching #{key} to #{restore_file.path}"
      s3_connection.bucket(@fu_conf[:s3_bucket]).get(key) do |chunk|
        fh.write chunk
      end
    end

    if(@fu_conf[:disable_compression])
      restore_file_unpacked = restore_file
    else
      restore_file_unpacked = Tempfile.new('restore.sql')

      cmd = niceify "tar xfz #{restore_file.path} -O > #{restore_file_unpacked.path}"
      puts "\nUntar: #{cmd}\n" if @verbose
      `#{cmd}`
    end
    
    prepare_db_for_restore

    # Do the actual restore
    case @db_conf[:adapter]
    when 'postgresql'
      cmd = niceify "export #{pgpassword_prefix} && #{restore_command_path} --clean #{sqlcmd_options} --dbname=#{@db_conf[:database]} #{restore_file_unpacked.path}"
    # when 'mysql'
    #   raise "restore unimplemented for #{}
    #   cmd = niceify "mysql command goes here"
    end
    puts "\nRestore: #{cmd}\n" if @verbose
    `#{cmd}`
  end

  ## Static-file Dump/Backup methods
  
  def dump_static
    if !@fu_conf[:static_paths]
      raise BackupFuConfigError, 'No static paths are defined in config/backup_fu.yml.  See README.'
    end
    paths = @fu_conf[:static_paths].split(' ')
    compress_static(paths)
  end
  
  def backup_static
    dump_static
    
    file = final_static_dump_path()
    puts "\nBacking up Static files to S3: #{file}\n" if @verbose
    
    store_file(file)
  end

  def cleanup
    count = @fu_conf[:keep_backups].to_i
    backups = Dir.glob("#{dump_base_path}/*.{sql}")
    if count >= backups.length
      puts "no old backups to cleanup"
    else
      puts "keeping #{count} of #{backups.length} backups"

      files_to_remove = backups - backups.last(count)

      if(!@fu_conf[:disable_compression])
        if(@fu_conf[:compressor] == 'zip')
          files_to_remove = files_to_remove.concat(Dir.glob("#{dump_base_path}/*.{zip}")[0, files_to_remove.length])
        else
          files_to_remove = files_to_remove.concat(Dir.glob("#{dump_base_path}/*.{gz}")[0, files_to_remove.length])
        end
      end

      files_to_remove.each do |f|
        File.delete(f)
      end

    end
  end
  
  private
  
  def s3
    @s3 ||= RightAws::S3.new(@fu_conf[:aws_access_key_id],
                             @fu_conf[:aws_secret_access_key])
  end
  
  def s3_bucket
    @s3_bucket ||= s3.bucket(@fu_conf[:s3_bucket], true, 'private')
  end
  
  def store_file(file)
    key = s3_bucket.key(File.basename(file))
    key.data = open(file)
    key.put(nil, 'private')
  end
  
  def s3_connection
    @s3 ||= begin
      RightAws::S3.new(@fu_conf[:aws_access_key_id], @fu_conf[:aws_secret_access_key])
    end
  end

  def check_conf
    @fu_conf[:s3_bucket] = ENV['s3_bucket'] unless ENV['s3_bucket'].blank?
    if @fu_conf[:app_name] == 'replace_me'
      raise BackupFuConfigError, 'Application name (app_name) key not set in config/backup_fu.yml.'
    elsif @fu_conf[:s3_bucket] == 'some-s3-bucket'
      raise BackupFuConfigError, 'S3 bucket (s3_bucket) not set in config/backup_fu.yml.  This bucket must be created using an external S3 tool like S3 Browser for OS X, or JetS3t (Java-based, cross-platform).'
    else
      # Check for access keys set as environment variables:
      if ENV.keys.include?('AMAZON_ACCESS_KEY_ID') && ENV.keys.include?('AMAZON_SECRET_ACCESS_KEY')
        @fu_conf[:aws_access_key_id] = ENV['AMAZON_ACCESS_KEY_ID']
        @fu_conf[:aws_secret_access_key] = ENV['AMAZON_SECRET_ACCESS_KEY']
      elsif @fu_conf[:aws_access_key_id].blank? || @fu_conf[:aws_access_key_id].include?('--replace me') || @fu_conf[:aws_secret_access_key].include?('--replace me')
        raise BackupFuConfigError, 'AWS Access Key Id or AWS Secret Key not set in config/backup_fu.yml.'
      end
    end
  end
  
  #! dump_path is totally the wrong name here
  def dump_path
    dump = {:postgresql => 'pg_dump',:mysql => 'mysqldump'}
    # Note: the 'mysqldump_path' config option is DEPRECATED but keeping this in for legacy config file support
    @fu_conf[:mysqldump_path] || @fu_conf[:dump_path] || dump[@db_conf[:adapter].intern]
  end

  def restore_command_path
    command = @fu_conf[:restore_command_path] || ((adapter = @db_conf[:adapter]) == 'postgresql' && 'pg_restore')
    raise "Restore unimplemented for adapter #{adapter}" if command.blank?
    command
  end

  def dump_base_path
    @fu_conf[:dump_base_path] || File.join(RAILS_ROOT, 'tmp', 'backup')
  end
  
  def db_filename
    "#{@fu_conf[:app_name]}_#{ @timestamp }_db.sql"
  end

  def db_filename_compressed
    if(@fu_conf[:compressor] == 'zip')
      db_filename.gsub('.sql', '.zip')
    else
      db_filename.gsub('.sql', '.tar')
    end
  end

  def final_db_dump_path
    if(@fu_conf[:disable_compression])
      filename = db_filename
    else
      if(@fu_conf[:compressor] == 'zip')
        filename = db_filename.gsub('.sql', '.zip')
      else
        filename = db_filename.gsub('.sql', '.tar.gz')
      end
    end
    File.join(dump_base_path, filename)
  end

  def static_compressed_path
    if(@fu_conf[:compressor] == 'zip')
      f = "#{@fu_conf[:app_name]}_#{ @timestamp }_static.zip"
    else
      f = "#{@fu_conf[:app_name]}_#{ @timestamp }_static.tar"
    end
    File.join(dump_base_path, f)
  end

  def final_static_dump_path
    if(@fu_conf[:compressor] == 'zip')
      f = "#{@fu_conf[:app_name]}_#{ @timestamp }_static.zip"
    else
      f = "#{@fu_conf[:app_name]}_#{ @timestamp }_static.tar.gz"
    end
    File.join(dump_base_path, f)
  end

  def create_dirs
    ensure_directory_exists(dump_base_path)
  end
  
  def ensure_directory_exists(dir)
    FileUtils.mkdir_p(dir) unless File.exist?(dir)
  end
  
  def niceify(cmd)
    if @fu_conf[:enable_nice]
      "nice -n -#{@fu_conf[:nice_level]} #{cmd}"
    else
      cmd
    end
  end

  def datetime_formatted
    Time.now.strftime("%Y-%m-%d") + "_#{ Time.now.tv_sec }"
  end
  
  def compress_db(dump_base_path, db_filename)
    compressed_path = File.join(dump_base_path, db_filename_compressed)

    if(@fu_conf[:compressor] == 'zip')
      cmd = niceify "zip #{zip_switches} #{compressed_path} #{dump_base_path}/#{db_filename}"
      puts "\nZip: #{cmd}\n" if @verbose
      `#{cmd}`
    else

      # TAR it up
      cmd = niceify "tar -cf #{compressed_path} -C #{dump_base_path} #{db_filename}"
      puts "\nTar: #{cmd}\n" if @verbose
      `#{cmd}`

      # GZip it up
      cmd = niceify "gzip -f #{compressed_path}"
      puts "\nGzip: #{cmd}" if @verbose
      `#{cmd}`
    end
  end

  def compress_static(paths)
    path_num = 0
    paths.each do |p|
      if p.first != '/'
        # Make into an Absolute path:
        p = File.join(RAILS_ROOT, p)
      end

      puts "Static Path: #{p}" if @verbose

      if @fu_conf[:compressor] == 'zip'
        cmd = niceify "zip -r #{zip_switch} #{static_compressed_path} #{p}"
        puts "\nZip: #{cmd}\n" if @verbose
        `#{cmd}`
      else
        if path_num == 0
          tar_switch = 'c'  # for create
        else
          tar_switch = 'r'  # for append
        end

        # TAR
        cmd = niceify "tar -#{tar_switch}f #{static_compressed_path} #{p}"
        puts "\nTar: #{cmd}\n" if @verbose
        `#{cmd}`

        path_num += 1

        # GZIP
        cmd = niceify "gzip -f #{static_compressed_path}"
        puts "\nGzip: #{cmd}" if @verbose
        `#{cmd}`
      end
    end
  end


  # Add -j option to keep from preserving directory structure
  def zip_switches
    if(@fu_conf[:zip_password] && !@fu_conf[:zip_password].blank?)
      password_option = "-P #{@fu_conf[:zip_password]}"
    else
      password_option = ''
    end
    
    "-j #{password_option}"
  end

  def skips
    return '' unless @fu_conf[:skips]

    raise BackupFuConfigError, 'skip option is not array or string' unless @fu_conf[:skips].kind_of?(Array) || @fu_conf[:skips].kind_of?(String)

    if @fu_conf[:skips].kind_of?(Array)
      @fu_conf[:skips].collect{|skip| " --exclude=#{skip} " }.join
    else
      @fu_conf[:skips]
    end
  end
end
