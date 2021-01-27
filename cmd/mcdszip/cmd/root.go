// Copyright © 2021 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/materials-commons/gomcdb/mcmodel"

	"github.com/apex/log"
	mcdb "github.com/materials-commons/gomcdb"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	cfgFile   string
	datasetID int
	zipfile   string
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "mcdszip",
	Short: "A brief description of your application",
	Long: `A longer description that spans multiple lines and likely contains
examples and usage of using your application. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		dsn := mcdb.MakeDSNFromEnv()
		db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			log.Fatalf("Unable to open database: %s", err)
		}

		var ds mcmodel.Dataset
		if err := db.Find(&ds, datasetID).Error; err != nil {
			log.Fatalf("Unable to find dataset %d: %s", datasetID, err)
		}

		createDatasetZipfile(db, &ds)
	},
}

func createDatasetZipfile(db *gorm.DB, ds *mcmodel.Dataset) {
	mcfsDir := viper.GetString("MCFS_DIR")

	zipfileFd, err := createZipfile()
	if err != nil {
		log.Fatalf("Unable to create zipfile: %s", err)
	}
	defer zipfileFd.Close()

	archive := zip.NewWriter(zipfileFd)
	defer archive.Close()

	dsFileSelector := mcdb.NewDatasetFileSelector(*ds)
	if err := dsFileSelector.LoadEntityFiles(db); err != nil {
		log.Errorf("Unable to load entity files for dataset %d: %s", ds.ID, err)
	}

	var files []mcmodel.File
	ds.GetFiles(db).FindInBatches(&files, 1000, func(tx *gorm.DB, batch int) error {
		for _, file := range files {
			if !includeFileInArchive(file, dsFileSelector) {
				continue
			}

			zipPath := strings.TrimPrefix(filepath.Join(file.Directory.Path, file.Name), "/")
			f, err := os.Open(file.ToPath(mcfsDir))
			if err != nil {
				log.Errorf("Unable to open file '%s' for achive: %s", file.ToPath(mcfsDir), err)
				continue
			}

			zipWriter, err := archive.Create(zipPath)
			if err != nil {
				log.Errorf("Unable to add file '%s' to zipfile: %s", zipPath, err)
				continue
			}

			if _, err := io.Copy(zipWriter, f); err != nil {
				log.Errorf("Unable to write file '%d' to zipfile: %s", file.ID, err)
				continue
			}
		}

		return nil
	})
}

func createZipfile() (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(zipfile), 0777); err != nil {
		return nil, err
	}

	return os.Create(zipfile)
}

func includeFileInArchive(file mcmodel.File, dsFileSelector *mcdb.DatasetFileSelector) bool {
	if !file.IsFile() {
		return false
	}

	return dsFileSelector.IsIncludedFile(filepath.Join(file.Directory.Path, file.Name))
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.mcdszip.yaml)")
	rootCmd.PersistentFlags().IntVarP(&datasetID, "dataset-id", "d", -1, "Dataset ID to build zipfile for")
	rootCmd.PersistentFlags().StringVarP(&zipfile, "zipfile", "z", "", "Path to write zipfile to")

	if mcfsDir := os.Getenv("MCFS_DIR"); mcfsDir == "" {
		log.Fatalf("MCFS_DIR not set")
	}
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".mcdszip" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".env")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}
