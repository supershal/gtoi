package main

import (
	"fmt"
	"time"

	"github.com/spf13/cobra"
	"github.com/supershal/gtoi"
)

var cfgFile string
var whisperDir string
var gtoiCmd = &cobra.Command{
	Use:   "gtoi",
	Short: "Migrate graphite data to influxdb",
	Long:  `Reads graphite whisper files and converts graphite points to influxdb points. Send influxdb points to influxdb instances using HTTP`,
	RunE:  run,
}

func init() {
	gtoiCmd.Flags().StringVarP(&cfgFile, "config", "c", "", "<PATH>/<FILE_NAME>.toml. Explicitly define the path, name and extension (toml) of the config file.")
	gtoiCmd.Flags().StringVarP(&whisperDir, "whisperDir", "w", "", "<PATH> to the root of the whisper database")
	// set executable function.
	gtoiCmd.RunE = run
}

func run(cmd *cobra.Command, args []string) error {
	if cfgFile == "" {
		return fmt.Errorf("Please provide config file")
	}

	if whisperDir == "" {
		return fmt.Errorf("Please provide path to whisper database")
	}

	var cfg *gtoi.Config
	cfg, err := gtoi.DecodeConfig(cfgFile)

	if err != nil {
		return err
	}

	//fmt.Printf("config = %+v\n", cfg.PointConverters.WspToInfConvs[0])
	t1 := time.Now()
	gtoi.Migrate(cfg, whisperDir)
	fmt.Println("Migration duration", time.Now().Sub(t1))

	return nil
}

func main() {
	gtoiCmd.Execute()
}
