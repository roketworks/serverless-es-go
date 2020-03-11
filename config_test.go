package serverless_es_go

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
	"strings"
)

type Config struct {
	Aws struct {
		Region       string
		AccessKey    string `mapstructure:"access_key"`
		AccessSecret string `mapstructure:"access_secret"`

		DynamoDb struct {
			Endpoint string

			Es struct {
				TableName string `mapstructure:"table_name"`
			}

			Sequences struct {
				TableName string `mapstructure:"table_name"`
			}
		}

		Sqs struct {
			Endpoint string
		}
	}
	Postgres struct {
		ConnectionString string `mapstructure:"connection_string"`
	}
	Projections struct {
		QueueNames []string `mapstructure:"queue_names"`
	}
}

var testConfig Config

func init() {
	viper.SetEnvPrefix("es_test")
	viper.AutomaticEnv()
	viper.EnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigName("config_test")
	viper.SetConfigType("toml")
	viper.AddConfigPath("../test")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	viper.Unmarshal(&testConfig)
}
