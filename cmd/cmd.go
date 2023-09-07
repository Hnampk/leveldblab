package cmd

import (
	"leveldblab/usecase/usecase1"
	"leveldblab/usecase/usecase3"
	"log"

	"time"

	"github.com/spf13/cobra"
)

// serveCmd represents the serve command
var RootCmd = &cobra.Command{
	Use:   "",
	Short: "",
	Long:  "",
}

var Usecase1Cmd = &cobra.Command{
	Use:   "usecase1",
	Short: "Backup trên cùng luồng",
	Run: func(cmd *cobra.Command, args []string) {
		write, err := cmd.Flags().GetInt("write")
		if err != nil {
			log.Fatalf("Cannot find config write")
		}
		read, err := cmd.Flags().GetInt("read")
		if err != nil {
			log.Fatalf("Cannot find config read")
		}
		duration, err := cmd.Flags().GetDuration("duration")
		if err != nil {
			log.Fatalf("Cannot find config duration")
		}
		usecase1.LevelDBMainTempTesting(read, write, duration)
	},
}

var Usecase3Cmd = &cobra.Command{
	Use:   "usecase3",
	Short: "Leveldb bình thường",
	Run: func(cmd *cobra.Command, args []string) {
		write, err := cmd.Flags().GetInt("write")
		if err != nil {
			log.Fatalf("Cannot find config write")
		}
		read, err := cmd.Flags().GetInt("read")
		if err != nil {
			log.Fatalf("Cannot find config read")
		}
		duration, err := cmd.Flags().GetDuration("duration")
		if err != nil {
			log.Fatalf("Cannot find config duration")
		}
		usecase3.LevelDBNormalTesting(read, write, duration)
	},
}

func init() {
	Usecase1Cmd.Flags().Int("write", 10, "write")
	Usecase1Cmd.Flags().Int("read", 10, "read")
	Usecase1Cmd.Flags().Duration("duration", 10*time.Second, "duration")
	RootCmd.AddCommand(Usecase1Cmd)

	Usecase3Cmd.Flags().Int("write", 10, "write")
	Usecase3Cmd.Flags().Int("read", 10, "read")
	Usecase3Cmd.Flags().Duration("duration", 10*time.Second, "duration")
	RootCmd.AddCommand(Usecase3Cmd)
}
