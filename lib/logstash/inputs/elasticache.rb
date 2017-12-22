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

  config :source_type, :validate => :string, :required => true
  config :source_name, :validate => :string, :required => true
  config :polling_frequency, :validate => :number, :default => 600

  def register
    require "aws-sdk"
    @logger.info "Registering ElastiCache input", :region => @region, :source_type => @source_type, :source_name => @source_name
    @elasticache = Aws::ElastiCache::Client.new aws_options_hash
    @since = DateTime.now - 13 # FIXME sincedb
  end

  def run(queue)
    @thread = Thread.current
    Stud.interval(@polling_frequency) do
      with_rescue_and_retry do
        checkpoint = Time.now

        more = true
        marker = nil
        while more do
          response = @elasticache.describe_events({
            source_identifier: @source_name,
            source_type: @source_type,
            start_time: @since,
            end_time: checkpoint,
            marker: marker,
          })

          response[:events].each { |item| enqueue item, queue }
          more = response[:marker]
          marker = response[:marker]
        end
        @since = checkpoint
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
    event.set @source_type.gsub(/-/, "_"), @source_name
    @logger.debug "shipping #{event} to #{queue}"
    queue << event
  end
end
