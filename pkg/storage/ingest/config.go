// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"errors"
	"flag"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	consumeFromLastOffset = "last-offset"
	consumeFromStart      = "start"
	consumeFromEnd        = "end"
	consumeFromTimestamp  = "timestamp"

	kafkaConfigFlagPrefix          = "ingest-storage.kafka"
	targetConsumerLagAtStartupFlag = kafkaConfigFlagPrefix + ".target-consumer-lag-at-startup"
	maxConsumerLagAtStartupFlag    = kafkaConfigFlagPrefix + ".max-consumer-lag-at-startup"
)

var (
	ErrMissingKafkaAddress               = errors.New("the Kafka address has not been configured")
	ErrMissingKafkaTopic                 = errors.New("the Kafka topic has not been configured")
	ErrInvalidWriteClients               = errors.New("the configured number of write clients is invalid (must be greater than 0)")
	ErrInvalidConsumePosition            = errors.New("the configured consume position is invalid")
	ErrInvalidProducerMaxRecordSizeBytes = fmt.Errorf("the configured producer max record size bytes must be a value between %d and %d", minProducerRecordDataBytesLimit, maxProducerRecordDataBytesLimit)
	ErrInconsistentConsumerLagAtStartup  = fmt.Errorf("the target and max consumer lag at startup must be either both set to 0 or to a value greater than 0")
	ErrInvalidMaxConsumerLagAtStartup    = fmt.Errorf("the configured max consumer lag at startup must greater or equal than the configured target consumer lag")

	consumeFromPositionOptions = []string{consumeFromLastOffset, consumeFromStart, consumeFromEnd, consumeFromTimestamp}
)

type Config struct {
	Enabled     bool            `yaml:"enabled"`
	KafkaConfig KafkaConfig     `yaml:"kafka"`
	Migration   MigrationConfig `yaml:"migration"`
}

func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&cfg.Enabled, "ingest-storage.enabled", false, "True to enable the ingestion via object storage.")

	cfg.KafkaConfig.RegisterFlagsWithPrefix(kafkaConfigFlagPrefix, f)
	cfg.Migration.RegisterFlagsWithPrefix("ingest-storage.migration", f)
}

// Validate the config.
func (cfg *Config) Validate() error {
	// Skip validation if disabled.
	if !cfg.Enabled {
		return nil
	}

	if err := cfg.KafkaConfig.Validate(); err != nil {
		return err
	}

	return nil
}

// KafkaConfig holds the generic config for the Kafka backend.
type KafkaConfig struct {
	Address      string        `yaml:"address"`
	Topic        string        `yaml:"topic"`
	ClientID     string        `yaml:"client_id"`
	DialTimeout  time.Duration `yaml:"dial_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	WriteClients int           `yaml:"write_clients"`

	ConsumerGroup                     string        `yaml:"consumer_group"`
	ConsumerGroupOffsetCommitInterval time.Duration `yaml:"consumer_group_offset_commit_interval"`

	LastProducedOffsetPollInterval time.Duration `yaml:"last_produced_offset_poll_interval"`
	LastProducedOffsetRetryTimeout time.Duration `yaml:"last_produced_offset_retry_timeout"`

	ConsumeFromPositionAtStartup  string        `yaml:"consume_from_position_at_startup"`
	ConsumeFromTimestampAtStartup int64         `yaml:"consume_from_timestamp_at_startup"`
	TargetConsumerLagAtStartup    time.Duration `yaml:"target_consumer_lag_at_startup"`
	MaxConsumerLagAtStartup       time.Duration `yaml:"max_consumer_lag_at_startup"`

	AutoCreateTopicEnabled           bool `yaml:"auto_create_topic_enabled"`
	AutoCreateTopicDefaultPartitions int  `yaml:"auto_create_topic_default_partitions"`

	ProducerMaxRecordSizeBytes int `yaml:"producer_max_record_size_bytes"`
	ProducerMaxBufferedBytes   int `yaml:"producer_max_buffered_bytes"`

	WaitStrongReadConsistencyTimeout time.Duration `yaml:"wait_strong_read_consistency_timeout"`

	// Used when logging unsampled client errors. Set from ingester's ErrorSampleRate.
	FallbackClientErrorSampleRate int64 `yaml:"-"`
}

func (cfg *KafkaConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *KafkaConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&cfg.Address, prefix+".address", "", "The Kafka backend address.")
	f.StringVar(&cfg.Topic, prefix+".topic", "", "The Kafka topic name.")
	f.StringVar(&cfg.ClientID, prefix+".client-id", "", "The Kafka client ID.")
	f.DurationVar(&cfg.DialTimeout, prefix+".dial-timeout", 2*time.Second, "The maximum time allowed to open a connection to a Kafka broker.")
	f.DurationVar(&cfg.WriteTimeout, prefix+".write-timeout", 10*time.Second, "How long to wait for an incoming write request to be successfully committed to the Kafka backend.")
	f.IntVar(&cfg.WriteClients, prefix+".write-clients", 1, "The number of Kafka clients used by producers. When the configured number of clients is greater than 1, partitions are sharded among Kafka clients. A higher number of clients may provide higher write throughput at the cost of additional Metadata requests pressure to Kafka.")

	f.StringVar(&cfg.ConsumerGroup, prefix+".consumer-group", "", "The consumer group used by the consumer to track the last consumed offset. The consumer group must be different for each ingester. If the configured consumer group contains the '<partition>' placeholder, it is replaced with the actual partition ID owned by the ingester. When empty (recommended), Mimir uses the ingester instance ID to guarantee uniqueness.")
	f.DurationVar(&cfg.ConsumerGroupOffsetCommitInterval, prefix+".consumer-group-offset-commit-interval", time.Second, "How frequently a consumer should commit the consumed offset to Kafka. The last committed offset is used at startup to continue the consumption from where it was left.")

	f.DurationVar(&cfg.LastProducedOffsetPollInterval, prefix+".last-produced-offset-poll-interval", time.Second, "How frequently to poll the last produced offset, used to enforce strong read consistency.")
	f.DurationVar(&cfg.LastProducedOffsetRetryTimeout, prefix+".last-produced-offset-retry-timeout", 10*time.Second, "How long to retry a failed request to get the last produced offset.")

	f.StringVar(&cfg.ConsumeFromPositionAtStartup, prefix+".consume-from-position-at-startup", consumeFromLastOffset, fmt.Sprintf("From which position to start consuming the partition at startup. Supported options: %s.", strings.Join(consumeFromPositionOptions, ", ")))
	f.Int64Var(&cfg.ConsumeFromTimestampAtStartup, prefix+".consume-from-timestamp-at-startup", 0, fmt.Sprintf("Milliseconds timestamp after which the consumption of the partition starts at startup. Only applies when consume-from-position-at-startup is %s", consumeFromTimestamp))

	howToDisableConsumerLagAtStartup := fmt.Sprintf("Set both -%s and -%s to 0 to disable waiting for maximum consumer lag being honored at startup.", targetConsumerLagAtStartupFlag, maxConsumerLagAtStartupFlag)
	f.DurationVar(&cfg.TargetConsumerLagAtStartup, targetConsumerLagAtStartupFlag, 2*time.Second, "The best-effort maximum lag a consumer tries to achieve at startup. "+howToDisableConsumerLagAtStartup)
	f.DurationVar(&cfg.MaxConsumerLagAtStartup, maxConsumerLagAtStartupFlag, 15*time.Second, "The guaranteed maximum lag before a consumer is considered to have caught up reading from a partition at startup, becomes ACTIVE in the hash ring and passes the readiness check. "+howToDisableConsumerLagAtStartup)

	f.BoolVar(&cfg.AutoCreateTopicEnabled, prefix+".auto-create-topic-enabled", true, "Enable auto-creation of Kafka topic if it doesn't exist.")
	f.IntVar(&cfg.AutoCreateTopicDefaultPartitions, prefix+".auto-create-topic-default-partitions", 0, "When auto-creation of Kafka topic is enabled and this value is positive, Kafka's num.partitions configuration option is set on Kafka brokers with this value when Mimir component that uses Kafka starts. This configuration option specifies the default number of partitions that the Kafka broker uses for auto-created topics. Note that this is a Kafka-cluster wide setting, and applies to any auto-created topic. If the setting of num.partitions fails, Mimir proceeds anyways, but auto-created topics could have an incorrect number of partitions.")

	f.IntVar(&cfg.ProducerMaxRecordSizeBytes, prefix+".producer-max-record-size-bytes", maxProducerRecordDataBytesLimit, "The maximum size of a Kafka record data that should be generated by the producer. An incoming write request larger than this size is split into multiple Kafka records. We strongly recommend to not change this setting unless for testing purposes.")
	f.IntVar(&cfg.ProducerMaxBufferedBytes, prefix+".producer-max-buffered-bytes", 1024*1024*1024, "The maximum size of (uncompressed) buffered and unacknowledged produced records sent to Kafka. The produce request fails once this limit is reached. This limit is applied per Kafka client. 0 to disable the limit.")

	f.DurationVar(&cfg.WaitStrongReadConsistencyTimeout, prefix+".wait-strong-read-consistency-timeout", 20*time.Second, "The maximum allowed for a read requests processed by an ingester to wait until strong read consistency is enforced. 0 to disable the timeout.")
}

func (cfg *KafkaConfig) Validate() error {
	if cfg.Address == "" {
		return ErrMissingKafkaAddress
	}
	if cfg.Topic == "" {
		return ErrMissingKafkaTopic
	}
	if cfg.WriteClients < 1 {
		return ErrInvalidWriteClients
	}
	if !slices.Contains(consumeFromPositionOptions, cfg.ConsumeFromPositionAtStartup) {
		return ErrInvalidConsumePosition
	}
	if cfg.ConsumeFromPositionAtStartup == consumeFromTimestamp {
		// We only do a simple soundness check for the value be a millisecond precision timestamp.
		if cfg.ConsumeFromTimestampAtStartup < 1e12 {
			return fmt.Errorf("%w: configured timestamp must be a millisecond timestamp", ErrInvalidConsumePosition)
		}
	} else {
		if cfg.ConsumeFromTimestampAtStartup > 0 {
			return fmt.Errorf("%w: configured consume position must be set to %q", ErrInvalidConsumePosition, consumeFromTimestamp)
		}
	}
	if cfg.ProducerMaxRecordSizeBytes < minProducerRecordDataBytesLimit || cfg.ProducerMaxRecordSizeBytes > maxProducerRecordDataBytesLimit {
		return ErrInvalidProducerMaxRecordSizeBytes
	}
	if (cfg.TargetConsumerLagAtStartup != 0) != (cfg.MaxConsumerLagAtStartup != 0) {
		return ErrInconsistentConsumerLagAtStartup
	}
	if cfg.MaxConsumerLagAtStartup < cfg.TargetConsumerLagAtStartup {
		return ErrInvalidMaxConsumerLagAtStartup
	}

	return nil
}

// GetConsumerGroup returns the consumer group to use for the given instanceID and partitionID.
func (cfg *KafkaConfig) GetConsumerGroup(instanceID string, partitionID int32) string {
	if cfg.ConsumerGroup == "" {
		return instanceID
	}

	return strings.ReplaceAll(cfg.ConsumerGroup, "<partition>", strconv.Itoa(int(partitionID)))
}

// MigrationConfig holds the configuration used to migrate Mimir to ingest storage. This config shouldn't be
// set for any other reason.
type MigrationConfig struct {
	DistributorSendToIngestersEnabled bool `yaml:"distributor_send_to_ingesters_enabled"`
}

func (cfg *MigrationConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefix("", f)
}

func (cfg *MigrationConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.BoolVar(&cfg.DistributorSendToIngestersEnabled, prefix+".distributor-send-to-ingesters-enabled", false, "When both this option and ingest storage are enabled, distributors write to both Kafka and ingesters. A write request is considered successful only when written to both backends.")
}
