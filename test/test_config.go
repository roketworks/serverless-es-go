package test

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
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
	}
	Postgres struct {
		ConnectionString string `mapstructure:"connection_string"`
	}
}

var Configuration Config

func init() {
	viper.SetEnvPrefix("ES_TEST_")
	viper.AutomaticEnv()
	viper.SetConfigName("settings")
	viper.SetConfigType("toml")
	viper.AddConfigPath("../test")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	viper.Unmarshal(&Configuration)
}
