package main

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"sort"
	"time"
)

var TimeForSleep = 10 * time.Second
var UrlCritical = "http://worker1.local/"
var UrlInfo = "http://worker2.local/"

type Queries struct {
	Critical map[int64]string
	Info     map[int64]string
}

func (q *Queries) InitQueries() {
	q.Critical = make(map[int64]string)
	q.Info = make(map[int64]string)
}

func (q *Queries) AddCritical(text string, w http.ResponseWriter) {
	mkTime := time.Now().UnixNano()
	q.Critical[mkTime] = text
}

func (q *Queries) AddInfo(text string) {
	mkTime := time.Now().UnixNano()
	q.Info[mkTime] = text
}

func (q *Queries) infoWorker() {

	for {
		lenght := len(q.Info)
		if lenght <= 0 {
			time.Sleep(TimeForSleep)
		}
		if lenght > 0 {
			keys := SortQueries(q.Info, lenght)

			for _, k := range keys {
				Send(UrlInfo, q.Info[k])
				delete(q.Info, k)
			}

			time.Sleep(TimeForSleep)
		}
	}
}

func (q *Queries) criticalWorker() {

	for {
		lenght := len(q.Critical)
		if lenght <= 0 {
			time.Sleep(TimeForSleep)
		}
		if lenght > 0 {

			keys := SortQueries(q.Critical, lenght)

			for _, k := range keys {
				Send(UrlCritical, q.Critical[k])
				delete(q.Critical, k)
			}

			time.Sleep(TimeForSleep)
		}
	}
}

func Send(url string, text string) {
	body := []byte("t=" + text)
	http.Post(url, "application/x-www-form-urlencoded", bytes.NewBuffer(body))

}

func SortQueries(array map[int64]string, lenght int) []int64 {

	keys := make([]int64, 0, lenght)

	for k := range array {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	return keys
}

var Qm Queries = *new(Queries)

func main() {
	Qm.InitQueries()

	go Qm.criticalWorker()
	go Qm.infoWorker()

	http.HandleFunc("/", Handler)

	log.Println("Starting MQ server on :1234")
	log.Fatal(http.ListenAndServe(":1234", nil))
}

func Handler(w http.ResponseWriter, r *http.Request) {
	t := r.FormValue("type")
	text := r.FormValue("text")

	if len(t) <= 0 {
		http.Error(w, "'type' param is required", 400)
	}
	if len(text) <= 0 {
		http.Error(w, "'text' param is required", 400)
	}

	if len(text) > 0 && len(t) > 0 {
		if t == "critical" {
			Qm.AddCritical(text, w)
		}
		if t == "info" {
			Qm.AddInfo(text)
		}
	}

	io.WriteString(w, "OK")
}
