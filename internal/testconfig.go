package internal

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
			Endpoint  string
			TableName string `mapstructure:"table_name"`
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

var TestConfig Config

func init() {
	viper.SetEnvPrefix("es_test")
	viper.AutomaticEnv()
	viper.EnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigName("testconfig")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("internal")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	viper.Unmarshal(&TestConfig)
}
