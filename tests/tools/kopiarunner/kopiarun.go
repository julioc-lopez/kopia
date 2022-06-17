// Package kopiarunner wraps the execution of the kopia binary.
package kopiarunner

import (
	"bytes"
	"errors"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const (
	defaultRepoPassword = "qWQPJ2hiiLgWRRCr"
)

// Runner is a helper for running kopia commands.
type Runner struct {
	Exe         string
	ConfigDir   string
	fixedArgs   []string
	environment []string
	tempDir     string
}

// ErrExeVariableNotSet is returned when the environment variable for the kopia
// executable is not set.
var ErrExeVariableNotSet = errors.New("KOPIA_EXE variable has not been set")

// RunnerOpts contains the options for creating a new runner.
type RunnerOpts struct {
	BaseDir      string
	Executable   string
	ConfigDir    string
	RepoPassword string
}

// NewRunner returns a newly initialized kopia runner.
func NewRunner(baseDir string) (*Runner, error) {
	return NewRunnerWithOptions(RunnerOpts{BaseDir: baseDir})
}

func NewRunnerWithOptions(opts RunnerOpts) (*Runner, error) {
	if opts.Executable == "" {
		exe := os.Getenv("KOPIA_EXE")
		if exe == "" {
			return nil, ErrExeVariableNotSet
		}

		opts.Executable = exe
	}

	var tempDir string

	if opts.ConfigDir == "" {
		configDir, err := os.MkdirTemp(opts.BaseDir, "kopia-config")
		if err != nil {
			return nil, err
		}

		tempDir = configDir
		opts.ConfigDir = configDir
	}

	if opts.RepoPassword == "" {
		opts.RepoPassword = defaultRepoPassword
	}

	return &Runner{
		Exe:       opts.Executable,
		ConfigDir: opts.ConfigDir,
		fixedArgs: []string{
			// use per-test config file, to avoid clobbering current user's setup.
			"--config-file", filepath.Join(opts.ConfigDir, "repository.config"),
		},
		environment: []string{"KOPIA_PASSWORD=" + opts.RepoPassword},
		tempDir:     tempDir,
	}, nil
}

// Cleanup cleans up the directories managed by the kopia Runner.
func (kr *Runner) Cleanup() {
	if kr.tempDir != "" {
		os.RemoveAll(kr.tempDir) //nolint:errcheck
	}
}

// Run will execute the kopia command with the given args.
func (kr *Runner) Run(args ...string) (stdout, stderr string, err error) {
	outB, errB, err2 := kr.RunBytes(args...)

	return string(outB), string(errB), err2
}

func (kr *Runner) RunBytes(args ...string) (stdout, stderr []byte, err error) {
	argsStr := strings.Join(args, " ")
	log.Printf("running '%s %v'", kr.Exe, argsStr)
	cmdArgs := append(append([]string(nil), kr.fixedArgs...), args...)
	c := exec.Command(kr.Exe, cmdArgs...)
	c.Env = append(os.Environ(), kr.environment...)

	errOut := &bytes.Buffer{}
	c.Stderr = errOut

	o, err := c.Output()
	log.Printf("finished '%s %v' with err=%v and output:\nSTDOUT:\n%s\nSTDERR:\n%s", kr.Exe, argsStr, err, o, errOut)

	return o, errOut.Bytes(), err
}

// RunAsync will execute the kopia command with the given args in background.
func (kr *Runner) RunAsync(args ...string) (*exec.Cmd, error) {
	log.Printf("running async '%s %v'", kr.Exe, strings.Join(args, " "))
	cmdArgs := append(append([]string(nil), kr.fixedArgs...), args...)
	//nolint:gosec //G204
	c := exec.Command(kr.Exe, cmdArgs...)
	c.Env = append(os.Environ(), kr.environment...)
	c.Stderr = &bytes.Buffer{}

	setpdeath(c)

	err := c.Start()
	if err != nil {
		return nil, err
	}

	return c, nil
}
