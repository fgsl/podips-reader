// =======================================================================================
// podips-reader: A program that reads logs about pods from Kubernetes and send to a queue
// @author Flávio Gomes da Silva Lisboa <flavio.lisboa@fgsl.eti.br>
// @license LGPL-2.1
// =======================================================================================
package main

import (
    "crypto/tls"
    "database/sql"
    "flag"
    "fmt"
    "io/ioutil"
    "net/http"
    "os"
    "path/filepath"
    "strconv"
    "strings"
    "time"

    apiv1 "k8s.io/api/core/v1"
    metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
    "k8s.io/apimachinery/pkg/watch"
    "k8s.io/client-go/kubernetes"
    "k8s.io/client-go/rest"
    "k8s.io/client-go/tools/clientcmd"
    stomp "github.com/go-stomp/stomp"
    "gopkg.in/ini.v1"
)

type PodInfo struct {
    eventType watch.EventType
    exitCode int
    phase string
    objectIP string
    objectName string
    objectNamespace string
    podKind string
    podStatus string
    sendLog bool
    state string
    terminated string
}

func main() {
    fmt.Println("PODIPS-READER: initializing audit for Kubernetes Pods")
    fmt.Println("PODIPS-READER: version 1.1.0")
    options := getOptions()

    config := getConfig(options)

    time.Sleep(30 * time.Second)

    clientset, err := kubernetes.NewForConfig(config)
    if err != nil {
        die("PODIPS-READER: ERROR: Can't create client configuration")
    }

    // continue even some error occurs
    for {
        // get pods in all the namespaces by omitting namespace
        // or specify namespace to get pods in particular namespace
        watch, err := clientset.CoreV1().Pods("").Watch(metav1.ListOptions{})
        if err == nil {
            http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}    
            resp, err := http.Get(getPodipsHost() + "/kubernetes/200/success")
            _ = resp
            if err != nil {
                fmt.Println("PODIPS-READER WATCH: WARNING: ", err.Error())
            }
            os.Create("/tmp/kubernetes_status")
            listEvents(watch)
        } else {
            fmt.Println("PODIPS-READER WATCH: WARNING: ", err.Error())
            http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}    
            resp, err := http.Get(getPodipsHost() + "/kubernetes/500/fail")
            _ = resp
            if err != nil {
                fmt.Println("PODIPS-READER WATCH: WARNING: ", err.Error())
            }
            os.Remove("/tmp/kubernetes_status")		
        }
        readMessagesFromDatabase()

        time.Sleep(10 * time.Second)
    }
}

func listEvents(w watch.Interface) {
    var pods = make(map[string]string)
    var data string
    var podInfo PodInfo
    var queue *stomp.Conn
    var err error

    for event := range w.ResultChan() {
        pod := event.Object.(*apiv1.Pod)
        podInfo.eventType = event.Type
        podInfo.objectIP = pod.Status.PodIP
        podInfo.objectName = pod.ObjectMeta.Name
        podInfo.objectNamespace = pod.ObjectMeta.Namespace
        podInfo.phase = fmt.Sprintf("%#v", pod.Status.Phase)
        podInfo = getPodStateTerminatedAndKind(podInfo, pod)

        index := podInfo.objectNamespace + "/" + podInfo.objectName

        podInfo = getPodStatusAndSendLog(podInfo, pods, index)

        if podInfo.sendLog {
            data = getDataForLog(podInfo)

            queue, err = getQueue()
	        err = queue.Send(
                "/queue/pods",// destination
                 "application/json",// content-type
                []byte(data))// body
            if err != nil {
                fmt.Println("ERROR WHEN SENDING TO QUEUE " + err.Error())
                http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}    
                resp, err := http.Get(getPodipsHost() + "/queue/write/500/fail")
                _ = resp
                if err != nil {
                    fmt.Println("PODIPS-READER MONITOR: WARNING: ", err.Error())
                }
                os.Remove("/tmp/queue_status")
                saveMessageToDatabase(data)
            } else {
                http.DefaultTransport.(*http.Transport).TLSClientConfig = &tls.Config{InsecureSkipVerify: true}    
                resp, err := http.Get(getPodipsHost() + "/queue/write/200/success")
                _ = resp
                if err != nil {
                    fmt.Println("PODIPS-READER MONITOR: WARNING: ", err.Error())
                }
		        os.Create("/tmp/queue_status")            
            }
            queue.Disconnect()
        }

        msg := "(send? " + strconv.FormatBool(podInfo.sendLog) + ")" + data

        fmt.Println(msg)
    }
}

// command-line arguments
func getOptions() map[string]bool {
    options := make(map[string]bool)
    var incluster *bool
    incluster = flag.Bool("incluster", true, "use in cluster, default is true")
    flag.Parse()
    options["incluster"] = *incluster
    return options
}

// Requires options["incluster"]
func getConfig(options map[string]bool) *rest.Config {
    var config *rest.Config
    var err error
    if options["incluster"] {
        config, err = rest.InClusterConfig()
        inPanic(err)
        fmt.Println("PODIPS-READER: Use config in cluster")
    } else {
        var kubeconfig string
        kubeconfig = filepath.Join(homeDir(), ".kube", "config")
        fmt.Println("PODIPS-READER: using config from: ", kubeconfig)
        // use the current context in kubeconfig
        config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
        inPanic(err)
        fmt.Println("PODIPS-READER: Use config out of cluster")
        if config == nil {
            var kubeApiserverURL string
            if os.Getenv("KUBE_APISERVER_URL") != "" {
                kubeApiserverURL = os.Getenv("KUBE_APISERVER_URL")
                fmt.Println("PODIPS-READER: using KUBE_APISERVER_URL")
            } else {
                die("PODIPS-READER: ERROR: KUBE_APISERVER_URL was not declared.")
            }
            config.Host = kubeApiserverURL
            config.TLSClientConfig.Insecure = false
            // Client Kubernetes Settings
            // Account Service:
            tokenSettings := getTokenSettings(options)
            token := tokenSettings["token"]
            _ = token
            caCertFile := tokenSettings["ca_cert_file"]
            _ = caCertFile
            config.TLSClientConfig.CAFile = caCertFile
            config.BearerToken = token
        }
    }
    return config
}

func getTokenSettings(options map[string]bool) map[string]string {
    settings := make(map[string]string)
    settings["token"] = ""
    settings["ca_cert_file"] = ""
    if _, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token"); err == nil {
        filetoken, err := os.Open("/var/run/secrets/kubernetes.io/serviceaccount/token")
        if err != nil {
            data := make([]byte, 100)
            token, err := filetoken.Read(data)
            if err != nil {
                _ = token // not always is used
                settings["ca_cert_file"] = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
            }
        }
    } else {
        if os.Getenv("KUBE_APITOKEN") != "" {
            settings["token"] = os.Getenv("KUBE_APITOKEN")
            if os.Getenv("KUBE_CA_CERT_FILE") != "" {
                settings["ca_cert_file"] = os.Getenv("KUBE_CA_CERT_FILE")
            } else {
                die("PODIPS-READER: ERROR: It was not possible to read certificate from Authority!")
            }
        } else {
            die("PODIPS-READER: ERROR: It was not possible to read ServiceAccount token!")
        }
    }
    fmt.Println("Using ca_cert_file = " + settings["ca_cert_file"])
    return settings
}

func getPodStateTerminatedAndKind(podInfo PodInfo, pod *apiv1.Pod) PodInfo {
    podInfo.exitCode = 999
    if len(pod.Status.ContainerStatuses) > 0 {
        podInfo.state = fmt.Sprintf("%#v", pod.Status.ContainerStatuses[0].State)
        podInfo.terminated = fmt.Sprintf("%#v", pod.Status.ContainerStatuses[0].State.Terminated)
        if strings.Contains(podInfo.terminated, "Completed") {
            podInfo.exitCode = int(pod.Status.ContainerStatuses[0].State.Terminated.ExitCode)
            podInfo.terminated = "Completed"
        } else {
            podInfo.terminated = "No terminated"
        }
    } else {
        podInfo.state = "No state"
        podInfo.terminated = "No terminated"
    }
    if len(pod.ObjectMeta.OwnerReferences) > 0 {
        podInfo.podKind = pod.ObjectMeta.OwnerReferences[0].Kind
    } else {
        podInfo.podKind = "Unknown"
    }
    return podInfo
}

func getPodStatusAndSendLog(podInfo PodInfo, pods map[string]string, index string) PodInfo {
    podInfo.sendLog = false
    if podInfo.eventType == "ADDED" {
        pods[index] = podInfo.objectIP
        if podInfo.objectIP != "" {
            podInfo.podStatus = "alreadyRunning"
            podInfo.sendLog = true
        } else {
            podInfo.podStatus = "new"
        }
    } else if podInfo.eventType == "MODIFIED" {
        other := pods[index]
        if strings.TrimSpace(other) == "" {
             other = "None"
        }
        if podInfo.objectIP != other {
            pods[index] = podInfo.objectIP
            podInfo.podStatus = "allocated"
            if podInfo.terminated == "Completed" && podInfo.exitCode == 0 {
                podInfo.podStatus = "deallocated"
            }
            if (podInfo.objectIP != "None"){
                podInfo.sendLog = true
            }
        }
    } else if podInfo.eventType == "DELETED" && podInfo.podKind == "ReplicaSet" {
        podInfo.podStatus = "deallocated"
        podInfo.sendLog = true
    }
    return podInfo
}

func getDataForLog(podInfo PodInfo) string {
    var data string

    hostname, err := os.Hostname()
    if err != nil {
        hostname := ""
     _ = hostname
    }
    logdc := "unknown"
    if os.Getenv("LOG_DC") != "" {
        logdc = os.Getenv("LOG_DC")
    }    
    now := time.Now()
    data = "{" +
        "\"class\": \"audit\"," +
        "\"subclass\": \"pod_ip\"," +
        "\"origin\":\"" +  hostname + "\"," +
        "\"dc\":\"" + logdc + "\"," +
        "\"host\":\"" +  hostname + "\"," +
        "\"pod_namespace\":\"" + podInfo.objectNamespace + "\"," +
        "\"pod\":\"" + podInfo.objectName + "\"," +
        "\"pod_ip\":\"" + podInfo.objectIP + "\"," +
        "\"pod_ip_status\":\"" + podInfo.podStatus + "\"," +
        "\"short_message\":\"" + podInfo.objectNamespace + "/" + podInfo.objectName + ":" + podInfo.objectIP + "\"," +
        "\"full_message\":\"" + podInfo.objectNamespace + "/"  + podInfo.objectName + ":" + podInfo.objectIP + ":" + podInfo.podStatus + ":" + now.String() + "\"," +
        "\"timestamp\":\"" +     now.String() + "\"," +
        "\"logtype\": \"kube-api-server\"}";
    return data;
}

func getPodipsHost() string {
    var podipsHost string
    data, err := ioutil.ReadFile("default_podips_host")
    if err == nil {
        podipsHost = string(data)
    } else {
        podipsHost = ""
    }
    if os.Getenv("PODIPS_HOST") != "" {
        podipsHost = os.Getenv("PODIPS_HOST")
    }
    return podipsHost
}

func getQueue() (*stomp.Conn, error) {
    queueHost := "podips-queue"
    if os.Getenv("QUEUE_HOST") != "" {
        queueHost = os.Getenv("QUEUE_HOST")
    }
    queuePort := "61616"
    if os.Getenv("QUEUE_PORT") != "" {
        queuePort = os.Getenv("QUEUE_PORT")
    }
    queueUsername := ""
    if os.Getenv("QUEUE_USERNAME") != "" {
        queueUsername = os.Getenv("QUEUE_USERNAME")
    }
    queuePassword := ""
    if os.Getenv("QUEUE_PASSWORD") != "" {
        queuePassword = os.Getenv("QUEUE_PASSWORD")
    }
    queueServer := queueHost + ":" + queuePort
    queue, err := stomp.Dial("tcp", queueServer,stomp.ConnOpt.Login(queueUsername, queuePassword))
    if err != nil {
        fmt.Println("ERROR: stomp.Dial: " + err.Error())
    } else {
        fmt.Println("PODIPS-READER: Has connectivity with " + queueServer)
    }    
    return queue, err
}

func getDb() (*sql.DB, error) {
    var dataSourceName string
    var db *sql.DB
    
    cfg, err := ini.Load("dbconfig.ini")
    if err != nil {
        return db, err
    }
    driver := cfg.Section("").Key("driver").String()
    host := cfg.Section("").Key("host").String()
    port := cfg.Section("").Key("port").String()
    user := cfg.Section("").Key("user").String()
    password := cfg.Section("").Key("password").String()
    dbName := cfg.Section("").Key("dbName").String()
    dataSourceName = fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbName)    
    db, err = sql.Open(driver,dataSourceName)
    return db,err
}

func saveMessageToDatabase(message string) {
    sqlStatement := fmt.Sprintf("INSERT INTO messages(message) VALUES ($1)")

    db, err := getDb()
    if err != nil {
        return  
    }
     
    insert, err := db.Prepare(sqlStatement)
    if err != nil {
        return
    }

    result, err := insert.Exec(message)
    checkError(err)

    if err == nil {
        affect, err := result.RowsAffected()
        checkError(err)
        if err == nil {
            fmt.Println(affect," message(s) saved into database")
            db.Close()
        }    
    }
}

func readMessagesFromDatabase() {
    db, err := getDb()
    if err != nil {
        return  
    }
    
    sqlStatement, err := db.Query("SELECT id, message FROM messages")
    if err != nil {
        fmt.Println(err.Error())
        return
    }

    queue, err := getQueue()
    for sqlStatement.Next() {

        var id int
        var message string

        err = sqlStatement.Scan(&id, &message)
        if err == nil {
            continue
        }
        err = queue.Send(
            "/queue/pods",// destination
            "application/json",// content-type
            []byte(message))// body
        if err != nil {
            fmt.Println("ERROR WHEN SENDING TO QUEUE " + err.Error())
        } else {
            sqlStatement := fmt.Sprintf("delete from messages where id=$1")

            delete, err := db.Prepare(sqlStatement)        
            if err != nil {
                continue
            }
            result, err := delete.Exec(id) // worst case, message will be replicate in queue
            if err != nil {
                affect, err := result.RowsAffected()
                fmt.Println(affect, " message deleted from table messages")
                checkError(err)
            }
        }       

        fmt.Printf("From database: %d\t%s\n", id, message)
    }
    queue.Disconnect()    
}

func homeDir() string {
    if h := os.Getenv("HOME"); h != "" {
        return h
    }
    return os.Getenv("USERPROFILE") // windows
}

// use this for aborting the program
func die(text string) {
    fmt.Println(text)
    os.Exit(1)
}

// use this for aborting the program
func inPanic(err error) {
    if err != nil {
        panic("FATAL ERROR " + err.Error())
    }
}

func checkError(err error) {
    if err != nil {
        fmt.Println("ERROR " + err.Error())
    }
}
