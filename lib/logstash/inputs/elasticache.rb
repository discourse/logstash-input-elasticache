# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/aws_config"
require "stud/interval"
require "aws-sdk"
require "date"

Aws.eager_autoload!

class LogStash::Inputs::ElastiCache < LogStash::Inputs::Base
  include LogStash::PluginMixins::AwsConfig::V2

  config_name "elasticache"
  milestone 1
  default :codec, "plain"

  config :source_type, :validate => :string, :default => nil
  config :source_name, :validate => :string, :default => nil
  config :polling_frequency, :validate => :number, :default => 600
  config :sincedb_path, :validate => :string, :default => nil

  def register
    require "aws-sdk"
    @logger.info "Registering ElastiCache input", :region => @region, :source_type => @source_type, :source_name => @source_name
    @elasticache = Aws::ElastiCache::Client.new aws_options_hash

    path = @sincedb_path || File.join(ENV["HOME"], ".sincedb_" + Digest::MD5.hexdigest("#{@source_type}+#{@source_name}"))
    @sincedb = SinceDB::File.new path
  end

  def run(queue)
    @thread = Thread.current
    Stud.interval(@polling_frequency) do
      with_rescue_and_retry do
        checkpoint = Time.now

        more = true
        marker = nil
        while more do
          options = {
            source_identifier: @source_name,
            source_type: @source_type,
            start_time: @sincedb.read,
            end_time: checkpoint,
            marker: marker,
          }.delete_if {|k,v| v.nil? }
          response = @elasticache.describe_events(options)
          response[:events].each { |item| enqueue item, queue }
          more = response[:marker]
          marker = response[:marker]
        end
        @sincedb.write checkpoint
      end
    end
  end

  def stop
    Stud.stop! @thread
  end

  def with_rescue_and_retry
    begin
      failures ||= 0
      yield
    rescue Aws::ElastiCache::Errors::ServiceError
      failures += 1
      if failures <= 4
        @logger.warn "service error for #{@source_type} #{@source_name}, delaying attempt ##{failures + 1}"
        sleep (2 ** failures)
        retry
      else
        @logger.error "pausing #{@source_type} #{@source_name} after five failures, will resume in #{@polling_frequency}s"
      end
    end
  end

  def enqueue(item, queue)
    event = LogStash::Event.new({
      "@timestamp" => item.date,
      "message" => item.message,
    })
    decorate event
    event.set @source_type.gsub(/-/, "_"), @source_name if @source_type
    @logger.debug "shipping #{event} to #{queue}"
    queue << event
  end

  private
  module SinceDB
    class File
      def initialize(file)
        @db = file
      end

      def read
        if ::File.exists?(@db)
          content = ::File.read(@db).chomp.strip
          return content.empty? ? Time.new : Time.parse(content)
        else
          return DateTime.now - 13
        end
      end

      def write(time)
        ::File.open(@db, 'w') { |file| file.write time.to_s }
      end
    end
  end
end
