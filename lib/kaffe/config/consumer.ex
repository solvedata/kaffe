defmodule Kaffe.Config.Consumer do
  import Kaffe.Config, only: [heroku_kafka_endpoints: 0, parse_endpoints: 1]

  def configuration(options \\ %{})
  def configuration(options) do
    Map.merge(default_configuration(),options)
  end
  def default_configuration() do
    default_config =
    %{
      endpoints: endpoints(),
      subscriber_name: nil,
      consumer_group: nil,
      topics: [],
      group_config: [
        offset_commit_policy: :commit_to_kafka_v2,
        offset_commit_interval_seconds: 5
      ],
      consumer_config: client_consumer_config(),
      message_handler: nil,
      async_message_ack: false,
      rebalance_delay_ms: 10_000,
      max_bytes: 1_000_000,
      min_bytes: 0,
      max_wait_time: 10_000,
      subscriber_retries: 5,
      subscriber_retry_delay_ms: 5_000,
      offset_reset_policy: :reset_by_subscriber,
      worker_allocation_strategy: :worker_per_partition
    }
    kaffe_config = Application.get_env(:kaffe, :consumer) |> Enum.into(%{})
    Map.merge(default_config, kaffe_config)
  end

  def endpoints do
    if heroku_kafka?() do
      heroku_kafka_endpoints()
    else
      parse_endpoints(config_get!(:endpoints))
    end
  end

  def client_consumer_config do
    default_client_consumer_config() ++ maybe_heroku_kafka_ssl() ++ sasl_options() ++ ssl_options()
  end

  def sasl_options do
    :sasl
    |> config_get(%{})
    |> Kaffe.Config.sasl_config()
  end

  def ssl_options do
    :ssl
    |> config_get(false)
    |> Kaffe.Config.ssl_config()
  end

  def default_client_consumer_config do
    [
      auto_start_producers: false,
      allow_topic_auto_creation: false,
      begin_offset: begin_offset()
    ]
  end

  def begin_offset do
    case config_get(:start_with_earliest_message, false) do
      true -> :earliest
      false -> -1
    end
  end

  def maybe_heroku_kafka_ssl do
    case heroku_kafka?() do
      true -> Kaffe.Config.ssl_config()
      false -> []
    end
  end

  def heroku_kafka? do
    config_get(:heroku_kafka_env, false)
  end

  def config_get!(key) do
    Application.get_env(:kaffe, :consumer)
    |> Keyword.fetch!(key)
  end

  def config_get(key, default) do
    Application.get_env(:kaffe, :consumer)
    |> Keyword.get(key, default)
  end
end
