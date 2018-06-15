package chronos

import (
	"errors"
	"fmt"
	"path"
	"strings"
	"time"
)

// Constants defining the various chronos endpoints
const (
	ChronosAPIJob             = "scheduler/job"
	ChronosAPIJobs            = "scheduler/jobs"
	ChronosAPIJobsSearch      = "scheduler/jobs/search"
	ChronosAPIKillJobTask     = "scheduler/task/kill"
	ChronosAPIAddScheduledJob = "scheduler/iso8601"
	ChronosAPIAddDependentJob = "scheduler/dependency"
)

// Chronoser chronos HTTP API interface
type Chronoser interface {
	Jobs() (Jobs, error)
	DeleteJob(name string) error
	DeleteJobTasks(name string) error
	StartJob(name string, args map[string]string) error
	AddScheduledJob(job *Job) error
	AddDependentJob(job *Job) error
	RunOnceNowJob(job *Job) error
	UnscheduleJob(job *Job) error
	SearchJobs(name string) (Jobs, error)
}

// Container chronos container struct
type Container struct {
	Type       string              `json:"type,omitempty"`
	Image      string              `json:"image,omitempty"`
	Network    string              `json:"network,omitempty"`
	Volumes    []map[string]string `json:"volumes,omitempty"`
	Parameters []map[string]string `json:"parameters,omitempty"`
}

// Job chronos job struct
// https://github.com/mesos/chronos/blob/master/docs/docs/api.md#job-configuration
type Job struct {
	Name                   string              `json:"name"`
	Command                string              `json:"command"`
	Shell                  bool                `json:"shell,omitempty"`
	Epsilon                string              `json:"epsilon,omitempty"`
	Executor               string              `json:"executor,omitempty"`
	ExecutorFlags          string              `json:"executorFlags,omitempty"`
	Retries                int                 `json:"retries,omitempty"`
	Owner                  string              `json:"owner,omitempty"`
	OwnerName              string              `json:"ownerName,omitempty"`
	Description            string              `json:"description,omitempty"`
	Async                  bool                `json:"async,omitempty"`
	SuccessCount           int                 `json:"successCount,omitempty"`
	ErrorCount             int                 `json:"errorCount,omitempty"`
	LastSuccess            string              `json:"lastSuccess,omitempty"`
	LastError              string              `json:"lastError,omitempty"`
	CPUs                   float32             `json:"cpus,omitempty"`
	Disk                   float32             `json:"disk,omitempty"`
	Mem                    float32             `json:"mem,omitempty"`
	Disabled               bool                `json:"disabled,omitempty"`
	SoftError              bool                `json:"softError,omitempty"`
	DataProcessingJobType  bool                `json:"dataProcessingJobType,omitempty"`
	ErrorsSinceLastSuccess int                 `json:"errorsSinceLastSuccess,omitempty"`
	URIs                   []string            `json:"uris,omitempty"`
	EnvironmentVariables   []map[string]string `json:"environmentVariables,omitempty"`
	Arguments              []string            `json:"arguments,omitempty"`
	HighPriority           bool                `json:"highPriority,omitempty"`
	RunAsUser              string              `json:"runAsUser,omitempty"`
	Container              *Container          `json:"container,omitempty"`
	Schedule               string              `json:"schedule,omitempty"`
	ScheduleTimeZone       string              `json:"scheduleTimeZone,omitempty"`
	Constraints            [][]string          `json:"constraints,omitempty"`
	Parents                []string            `json:"parents,omitempty"`
}

// Jobs slice of Job
type Jobs []Job

// FormatSchedule will return a chronos schedule that can be used by the job
// See https://github.com/mesos/chronos/blob/master/docs/docs/api.md#adding-a-scheduled-job for details
// startTime (time.Time): when you want the job to start. A zero time instant means start immediately.
// interval (string): How often to run the job.
// reps (string): How many times to run the job.
func FormatSchedule(startTime time.Time, interval string, reps string) (string, error) {
	if err := validateInterval(interval); err != nil {
		return "", err
	}

	if err := validateReps(reps); err != nil {
		return "", err
	}

	schedule := fmt.Sprintf("%s/%s/%s", reps, formatTimeString(startTime), interval)

	return schedule, nil
}

// RunOnceNowSchedule will return a schedule that starts immediately, runs once,
// and runs every 2 minutes until successful
func RunOnceNowSchedule() string {
	return "R1//PT2M"
}

// Jobs gets all jobs that chronos knows about
func (client *Chronos) Jobs() (Jobs, error) {
	jobs := new(Jobs)

	err := client.apiGet(ChronosAPIJobs, nil, jobs)

	if err != nil {
		return nil, err
	}

	return *jobs, nil
}

// SearchJobs gets a job that matches name
func (client *Chronos) SearchJobs(name string) (Jobs, error) {

	if len(strings.TrimSpace(name)) == 0 {
		return nil, errors.New("[SearchJobs] missing name argument")
	}

	jobs := new(Jobs)

	queryParams := map[string]string{"name": name}

	err := client.apiGet(ChronosAPIJobsSearch, queryParams, jobs)

	if err != nil {
		return nil, err
	}

	return *jobs, nil
}

// UnscheduleJob will delete a chronos job
// name: The name of job you wish to delete
func (client *Chronos) UnscheduleJob(job *Job) error {
	job.Schedule = "R0//PT0M"
	return client.apiPost(ChronosAPIAddScheduledJob, nil, job, nil)
}

// DeleteJob will delete a chronos job
// name: The name of job you wish to delete
func (client *Chronos) DeleteJob(name string) error {
	return client.apiDelete(path.Join(ChronosAPIJob, name), nil, nil)
}

// DeleteJobTasks will delete all tasks associated with a job.
// name: The name of the job whose tasks you wish to delete
func (client *Chronos) DeleteJobTasks(name string) error {
	return client.apiDelete(path.Join(ChronosAPIKillJobTask, name), nil, nil)
}

// StartJob can manually start a job
// name: The name of the job to start
// args: A map of arguments to append to the job's command
func (client *Chronos) StartJob(name string, args map[string]string) error {
	return client.apiPut(path.Join(ChronosAPIJob, name), args, nil)
}

// AddScheduledJob will add a scheduled job
// job: The job you would like to schedule
func (client *Chronos) AddScheduledJob(job *Job) error {
	return client.apiPost(ChronosAPIAddScheduledJob, nil, job, nil)
}

// AddDependentJob will add a dependent job
func (client *Chronos) AddDependentJob(job *Job) error {
	return client.apiPost(ChronosAPIAddDependentJob, nil, job, nil)
}

// RunOnceNowJob will add a scheduled job with a schedule generated by RunOnceNowSchedule
func (client *Chronos) RunOnceNowJob(job *Job) error {
	job.Schedule = RunOnceNowSchedule()
	job.Epsilon = "PT10M"
	return client.AddScheduledJob(job)
}

func validateReps(reps string) error {
	if strings.HasPrefix(reps, "R") {
		return nil
	}

	return errors.New("Repetitions string not formatted correctly")
}

func validateInterval(interval string) error {
	if strings.HasPrefix(interval, "P") {
		return nil
	}

	return errors.New("Interval string not formatted correctly")
}

func formatTimeString(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.Format(time.RFC3339Nano)
}
