package config

import (
	"fmt"
	"github.com/spf13/viper"
	"os"
	"strings"
)

func init() {
	viper.AutomaticEnv()
	viper.EnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigName("config")
	viper.SetConfigType("toml")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Ignore, don't need config file for every service
		} else {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}
