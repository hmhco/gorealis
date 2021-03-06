/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"flag"
	realis "github.com/paypal/gorealis"
	"github.com/paypal/gorealis/gen-go/apache/aurora"
	"github.com/paypal/gorealis/response"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"log"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var clientLogger = LevelLogger{
	logger: log.New(os.Stdout, "client: ", log.Ltime|log.Ldate|log.LUTC),
	}
var cmd, executor, url, clustersConfig, clusterName, updateId, username, password, zkUrl, hostList, role, name, stage string
var cpu float64
var ram, disk int64
var caCertsPath string
var clientKey, clientCert string
var debug bool
var ConnectionTimeout = 20000

func init() {
	flag.StringVar(&cmd, "cmd", "", "Job request type to send to Aurora Scheduler")
	flag.StringVar(&executor, "executor", "thermos", "Executor to use")
	flag.StringVar(&url, "url", "", "URL at which the Aurora Scheduler exists as [url]:[port]")
	flag.StringVar(&clustersConfig, "clusters", "", "Location of the clusters.json file used by aurora.")
	flag.StringVar(&clusterName, "cluster", "devcluster", "Name of cluster to run job on (only necessary if clusters is set)")
	flag.StringVar(&updateId, "updateId", "", "Update ID to operate on")
	flag.StringVar(&username, "username", os.Getenv("AURORA_API_USER"), "Username to use for authorization")
	flag.StringVar(&password, "password", os.Getenv("AURORA_API_PASSWORD"), "Password to use for authorization")
	flag.StringVar(&zkUrl, "zkurl", "", "zookeeper url")
	flag.StringVar(&hostList, "hostList", "", "Comma separated list of hosts to operate on")
	flag.StringVar(&role, "role", "", "owner role to use")
	flag.StringVar(&caCertsPath, "caCertsPath", "", "Path to CA certs on local machine.")
	flag.StringVar(&clientCert, "clientCert", "", "Client certificate to use to connect to Aurora.")
	flag.StringVar(&clientKey, "clientKey", "", "Client private key to use to connect to Aurora.")
	flag.StringVar(&name, "name", "", "Name of Aurora job for job specific commands")
	flag.StringVar(&stage, "stage", "", "Stage of Aurora job for job specific commands")
	flag.Float64Var(&cpu, "cpu", -1, "Number of CPU cores to apply to job during update")
	flag.Int64Var(&ram, "ram", -1, "Amount of RAM(GB) to apply to job during update")
	flag.Int64Var(&disk, "disk", -1, "Amount of RAM(GB) to apply to job during update")
	flag.BoolVar(&debug, "debug", false, "Control logging in script with debug mode")

	flag.Parse()

	// Attempt to load leader from zookeeper using a
	// cluster.json file used for the default aurora client if provided.
	// This will override the provided url in the arguments
	if clustersConfig != "" {
		clusters, err := realis.LoadClusters(clustersConfig)
		if err != nil {
			clientLogger.fatalPrintln(err)
		}

		cluster, ok := clusters[clusterName]
		if !ok {
			clientLogger.fatalPrintf("Cluster %s doesn't exist in the file provided\n", clusterName)
		}

		url, err = realis.LeaderFromZK(cluster)
		if err != nil {
			clientLogger.fatalPrintln(err)
		}
	}
}

type ExecutorConfig string

type MemoryConfig struct {
	MinMemory, MaxMemory int64
}

func (config ExecutorConfig) updateCmdline(memory MemoryConfig) ExecutorConfig{
	configVal := string(config)
	processList := gjson.Get(configVal, "task.processes")
	findCmdline := regexp.MustCompile("cmdline")
	cmdlineCount := len(findCmdline.FindAllStringIndex(processList.String(), -1))
	clientLogger.debugPrintln("Total processes count in task - ",cmdlineCount)
	for cmdLineItr:=0; cmdLineItr < cmdlineCount; cmdLineItr++ {
		cmdCount := strconv.Itoa(cmdLineItr)
		cmdline := gjson.Get(configVal, "task.processes."+cmdCount+".cmdline")
		if cmdline.String() != "" {
			switch {
			case strings.Contains(strings.ToLower(cmdline.String()),"java"):
				clientLogger.debugPrintln("Process ["+cmdCount+"] - DETECTED JAVA APPLICATION :: Modifying the cmdline for Java")
				if strings.Contains(strings.ToLower(cmdline.String()),"-xmx") {
					remax := regexp.MustCompile("(?i)-Xmx(.*?)m")
					remin := regexp.MustCompile("(?i)-Xms(.*?)m")
					clientLogger.debugPrintln("Process ["+cmdCount+"] - Existing Xmx: ", remax.FindStringSubmatch(cmdline.String())[1] , " Xms:", remin.FindStringSubmatch(cmdline.String())[1])
					clientLogger.debugPrintln("Process ["+cmdCount+"] - Updating Xmx: ", memory.MaxMemory , " Xms:", memory.MinMemory)
					modCmdLine := remax.ReplaceAllString(cmdline.String(), "-Xmx"+strconv.Itoa(int(memory.MaxMemory))+"m")
					modCmdLine = remin.ReplaceAllString(modCmdLine,"-Xms"+strconv.Itoa(int(memory.MinMemory))+"m")
					configVal,_ = sjson.Set(configVal, "task.processes."+cmdCount+".cmdline", modCmdLine)
				}else {
					clientLogger.debugPrintln("Process ["+cmdCount+"] - NO JAVA XMX OR XMS VALUE DETECTED :: Not modifying the cmdline for Java")
				}
				break
			default:
				clientLogger.debugPrintln("Process ["+cmdCount+"] - SKIPPING CMDLINE UPDATE FOR PROCESS")
			}
		}else{
			clientLogger.debugPrintln("Process ["+cmdCount+"] - SKIPPING : key cmdline is not found in processes")
		}
	}
	return ExecutorConfig(configVal)
}

func main() {

	var job realis.Job
	var err error
	var monitor *realis.Monitor
	var r realis.Realis

	clientOptions := []realis.ClientOption{
		realis.BasicAuth(username, password),
		realis.ThriftJSON(),
		realis.TimeoutMS(ConnectionTimeout),
		realis.BackOff(realis.Backoff{
			Steps:    2,
			Duration: 10 * time.Second,
			Factor:   2.0,
			Jitter:   0.1,
		}),
	}

	if debug {
		clientLogger.Println("DEBUG MODE : ON")
		clientLogger.EnableDebug(true)
		clientOptions = append(clientOptions, realis.Debug())
	}

	if zkUrl != "" {
		clientLogger.Println("zkUrl: ", zkUrl)
		clientOptions = append(clientOptions, realis.ZKUrl(zkUrl))
	} else {
		clientOptions = append(clientOptions, realis.SchedulerUrl(url))
	}

	if caCertsPath != "" {
		clientOptions = append(clientOptions, realis.Certspath(caCertsPath))
	}

	if clientKey != "" && clientCert != "" {
		clientOptions = append(clientOptions, realis.ClientCerts(clientKey, clientCert))
	}

	r, err = realis.NewRealisClient(clientOptions...)
	if err != nil {
		clientLogger.fatalPrintln(err)
	}
	monitor = &realis.Monitor{Client: r}
	defer r.Close()

	switch executor {
	case "thermos":
		job = realis.NewJob().
			Environment(stage).
			Role(role).
			Name(name).
			ExecutorName(aurora.AURORA_EXECUTOR_NAME).
			CPU(cpu).
			RAM(ram).
			Disk(disk).
			IsService(true).
			InstanceCount(1).
			AddPorts(1)
	case "none":
		job = realis.NewJob().
			Environment("prod").
			Role("vagrant").
			Name("docker_as_task").
			CPU(1).
			RAM(64).
			Disk(100).
			IsService(true).
			InstanceCount(1).
			AddPorts(1)
	default:
		clientLogger.fatalPrintln("Only thermos, compose, and none are supported for now")
	}

	switch cmd {
	case "descheduleCron":
		clientLogger.Println("Descheduling a Cron job")
		resp, err := r.DescheduleCronJob(job.JobKey())
		if err != nil {
			clientLogger.fatalPrint(err)
		}
		clientLogger.Println(resp.String())

	case "kill":
		clientLogger.Println("Killing job")

		resp, err := r.KillJob(job.JobKey())
		if err != nil {
			clientLogger.fatalPrint(err)
		}

		if ok, err := monitor.Instances(job.JobKey(), 0, 5, 50); !ok || err != nil {
			clientLogger.fatalPrint("Unable to kill all instances of job")
		}
		clientLogger.Println(resp.String())

	case "restart":
		clientLogger.Println("Restarting job")
		resp, err := r.RestartJob(job.JobKey())
		if err != nil {
			clientLogger.fatalPrint(err)
		}

		clientLogger.Println(resp.String())

	case "liveCount":
		clientLogger.Println("Getting instance count")

		live, err := r.GetInstanceIds(job.JobKey(), aurora.LIVE_STATES)
		if err != nil {
			clientLogger.fatalPrint(err)
		}

		clientLogger.Printf("Live instances: %+v\n", live)

	case "activeCount":
		clientLogger.Println("Getting instance count")

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			clientLogger.fatalPrint(err)
		}

		clientLogger.Println("Number of live instances: ", len(live))

	case "flexUp":
		clientLogger.Println("Flexing up job")

		numOfInstances := int32(4)

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			clientLogger.fatalPrint(err)
		}
		currInstances := int32(len(live))
		clientLogger.debugPrintln("Current num of instances: ", currInstances)
		resp, err := r.AddInstances(aurora.InstanceKey{
			JobKey:     job.JobKey(),
			InstanceId: live[0],
		},
			numOfInstances)

		if err != nil {
			clientLogger.fatalPrint(err)
		}

		if ok, err := monitor.Instances(job.JobKey(), currInstances+numOfInstances, 5, 50); !ok || err != nil {
			clientLogger.errorPrintln("Flexing up failed")
		}
		clientLogger.Println(resp.String())

	case "flexDown":
		clientLogger.Println("Flexing down job")

		numOfInstances := int32(2)

		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			clientLogger.fatalPrint(err)
		}
		currInstances := int32(len(live))
		clientLogger.Println("Current num of instances: ", currInstances)

		resp, err := r.RemoveInstances(job.JobKey(), numOfInstances)
		if err != nil {
			clientLogger.fatalPrint(err)
		}

		if ok, err := monitor.Instances(job.JobKey(), currInstances-numOfInstances, 5, 100); !ok || err != nil {
			clientLogger.errorPrintln("flexDown failed")
		}

		clientLogger.Println(resp.String())

	case "update":
		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			clientLogger.zfatalPrint(err)
		}
		currInstances := int32(len(live))
		if currInstances > 0 {
			taskConfig, err := r.FetchTaskConfig(aurora.InstanceKey{
				JobKey:     job.JobKey(),
				InstanceId: live[0],
			})
			if err != nil {
				clientLogger.zfatalPrint(err)
			}

			updateJob := realis.NewDefaultUpdateJob(taskConfig)
			updateJob.InstanceCount(currInstances)

			if ram != -1 {
				clientLogger.debugPrintln("RAM:", ram)
				executorConf := ExecutorConfig(taskConfig.ExecutorConfig.GetData())
				memValues := MemoryConfig{int64(math.Floor(float64(ram) * 0.2)) , int64(math.Floor(float64(ram) * 0.8))}
				executorConf = executorConf.updateCmdline(memValues)
				updateJob.ExecutorData(string(executorConf))
				clientLogger.Printf("Updating %s/%s/%s\n", role, stage, name)
				updateJob.RAM(ram)
			}

			if cpu != -1 {
				clientLogger.debugPrintln("CPU:", cpu)
				updateJob.CPU(cpu)
			}

			if disk != -1 {
				clientLogger.debugPrintln("DISK:", disk)
				updateJob.Disk(disk)
			}

			resp, err := r.StartJobUpdate(updateJob, "")
			if err != nil {
				clientLogger.zfatalPrint(err)
			}
			updateJobDetails := resp.GetDetails()
			if len(updateJobDetails) > 0 {
				clientLogger.errorPrintln(updateJobDetails[0].Message)
				clientLogger.errorPrintln("QUITTING !!")
			}else{
				clientLogger.Println("TRIGGERED JOB UPDATE SUCCESSFULLY")
				jobUpdateKey := response.JobUpdateKey(resp)
				monitor.JobUpdate(*jobUpdateKey, 5, 3600)
			}
		}else {
			clientLogger.errorPrintln("ZERO INSTANCES RUNNING")
			clientLogger.Println("SKIPPING RESOURCE UPDATE !!")
		}

	case "pauseJobUpdate":
		resp, err := r.PauseJobUpdate(&aurora.JobUpdateKey{
			Job: job.JobKey(),
			ID:  updateId,
		}, "")

		if err != nil {
			clientLogger.fatalPrint(err)
		}
		clientLogger.Println("PauseJobUpdate response: ", resp.String())

	case "resumeJobUpdate":
		resp, err := r.ResumeJobUpdate(&aurora.JobUpdateKey{
			Job: job.JobKey(),
			ID:  updateId,
		}, "")

		if err != nil {
			clientLogger.fatalPrint(err)
		}
		clientLogger.Println("ResumeJobUpdate response: ", resp.String())

	case "pulseJobUpdate":
		resp, err := r.PulseJobUpdate(&aurora.JobUpdateKey{
			Job: job.JobKey(),
			ID:  updateId,
		})
		if err != nil {
			clientLogger.fatalPrint(err)
		}

		clientLogger.Println("PulseJobUpdate response: ", resp.String())

	case "updateDetails":
		resp, err := r.JobUpdateDetails(aurora.JobUpdateQuery{
			Key: &aurora.JobUpdateKey{
				Job: job.JobKey(),
				ID:  updateId,
			},
			Limit: 1,
		})

		if err != nil {
			clientLogger.fatalPrint(err)
		}

		clientLogger.Println(response.JobUpdateDetails(resp))

	case "abortUpdate":
		clientLogger.Println("Abort update")
		resp, err := r.AbortJobUpdate(aurora.JobUpdateKey{
			Job: job.JobKey(),
			ID:  updateId,
		},
			"")

		if err != nil {
			clientLogger.fatalPrint(err)
		}
		clientLogger.Println(resp.String())

	case "rollbackUpdate":
		clientLogger.Println("Abort update")
		resp, err := r.RollbackJobUpdate(aurora.JobUpdateKey{
			Job: job.JobKey(),
			ID:  updateId,
		},
			"")

		if err != nil {
			clientLogger.fatalPrint(err)
		}
		clientLogger.Println(resp.String())

	case "taskConfig":
		clientLogger.Println("Getting job info")
		live, err := r.GetInstanceIds(job.JobKey(), aurora.ACTIVE_STATES)
		if err != nil {
			clientLogger.fatalPrint(err)

		}
		config, err := r.FetchTaskConfig(aurora.InstanceKey{
			JobKey:     job.JobKey(),
			InstanceId: live[0],
		})

		if err != nil {
			clientLogger.fatalPrint(err)
		}

		clientLogger.Println(config.String())

	case "updatesummary":
		clientLogger.Println("Getting job update summary")
		jobquery := &aurora.JobUpdateQuery{
			Role:   &job.JobKey().Role,
			JobKey: job.JobKey(),
		}
		updatesummary, err := r.GetJobUpdateSummaries(jobquery)
		if err != nil {
			clientLogger.fatalPrintf("error while getting update summary: %v", err)
		}

		clientLogger.Println(updatesummary)

	case "taskStatus":
		clientLogger.Println("Getting task status")
		taskQ := &aurora.TaskQuery{
			Role:        &job.JobKey().Role,
			Environment: &job.JobKey().Environment,
			JobName:     &job.JobKey().Name,
		}
		tasks, err := r.GetTaskStatus(taskQ)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n ", err)
		}

		clientLogger.Printf("length: %d\n ", len(tasks))
		clientLogger.Printf("tasks: %+v\n", tasks)

	case "tasksWithoutConfig":
		clientLogger.Println("Getting task status")
		taskQ := &aurora.TaskQuery{
			Role:        &job.JobKey().Role,
			Environment: &job.JobKey().Environment,
			JobName:     &job.JobKey().Name,
		}
		tasks, err := r.GetTasksWithoutConfigs(taskQ)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n ", err)
		}

		clientLogger.Printf("length: %d\n ", len(tasks))
		clientLogger.Printf("tasks: %+v\n", tasks)

	case "drainHosts":
		clientLogger.Println("Setting hosts to DRAINING")
		if hostList == "" {
			clientLogger.fatalPrint("No hosts specified to drain")
		}
		hosts := strings.Split(hostList, ",")
		_, result, err := r.DrainHosts(hosts...)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

		// Monitor change to DRAINING and DRAINED mode
		hostResult, err := monitor.HostMaintenance(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			5,
			10)
		if err != nil {
			for host, ok := range hostResult {
				if !ok {
					clientLogger.Printf("Host %s did not transtion into desired mode(s)\n", host)
				}
			}
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

		clientLogger.Print(result.String())

	case "SLADrainHosts":
		clientLogger.Println("Setting hosts to DRAINING using SLA aware draining")
		if hostList == "" {
			clientLogger.fatalPrint("No hosts specified to drain")
		}
		hosts := strings.Split(hostList, ",")

		policy := aurora.SlaPolicy{PercentageSlaPolicy: &aurora.PercentageSlaPolicy{Percentage: 50.0}}

		result, err := r.SLADrainHosts(&policy, 30, hosts...)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

		// Monitor change to DRAINING and DRAINED mode
		hostResult, err := monitor.HostMaintenance(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_DRAINED, aurora.MaintenanceMode_DRAINING},
			5,
			10)
		if err != nil {
			for host, ok := range hostResult {
				if !ok {
					clientLogger.Printf("Host %s did not transtion into desired mode(s)\n", host)
				}
			}
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

		clientLogger.Print(result.String())

	case "endMaintenance":
		clientLogger.Println("Setting hosts to ACTIVE")
		if hostList == "" {
			clientLogger.fatalPrint("No hosts specified to drain")
		}
		hosts := strings.Split(hostList, ",")
		_, result, err := r.EndMaintenance(hosts...)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

		// Monitor change to DRAINING and DRAINED mode
		hostResult, err := monitor.HostMaintenance(
			hosts,
			[]aurora.MaintenanceMode{aurora.MaintenanceMode_NONE},
			5,
			10)
		if err != nil {
			for host, ok := range hostResult {
				if !ok {
					clientLogger.Printf("Host %s did not transtion into desired mode(s)\n", host)
				}
			}
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

		clientLogger.Print(result.String())

	case "getPendingReasons":
		clientLogger.Println("Getting pending reasons")
		taskQ := &aurora.TaskQuery{
			Role:        &job.JobKey().Role,
			Environment: &job.JobKey().Environment,
			JobName:     &job.JobKey().Name,
		}
		reasons, err := r.GetPendingReason(taskQ)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n ", err)
		}

		clientLogger.Printf("length: %d\n ", len(reasons))
		clientLogger.Printf("tasks: %+v\n", reasons)

	case "getJobs":
		clientLogger.Println("GetJobs...role: ", role)
		_, result, err := r.GetJobs(role)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}
		clientLogger.Println("map size: ", len(result.Configs))
		clientLogger.Println(result.String())

	case "snapshot":
		clientLogger.Println("Forcing scheduler to write snapshot to mesos replicated log")
		err := r.Snapshot()
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

	case "performBackup":
		clientLogger.Println("Writing Backup of Snapshot to file system")
		err := r.PerformBackup()
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

	case "forceExplicitRecon":
		clientLogger.Println("Force an explicit recon")
		err := r.ForceExplicitTaskReconciliation(nil)
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

	case "forceImplicitRecon":
		clientLogger.Println("Force an implicit recon")
		err := r.ForceImplicitTaskReconciliation()
		if err != nil {
			clientLogger.fatalPrintf("error: %+v\n", err.Error())
		}

	default:
		clientLogger.fatalPrint("Command not supported")
	}
}
