package main

import "github.com/influxdb/influxdb/client"
import (
	"flag"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"strconv"
	"time"
)

const (
	MyDB          = "testing"
	MyMeasurement = "shapes"
)

func main() {
	var rps = flag.Int("rate", 1, "Requests per second")
	var runtime = flag.Int("seconds", 60, "Number of seconds to run load generator")
	flag.Parse()

	con, err := setupInflux()
	if err != nil {
		panic(err)
	}

	fmt.Println(time.Now())
	ticker := time.NewTicker(time.Second * 1)
	go func() {
		for _ = range ticker.C {
			go dispatchRequests(rps, con)
		}
	}()
	time.Sleep(time.Second * time.Duration(*runtime))
	ticker.Stop()
	fmt.Println("Ticker stopped")
}

func dispatchRequests(rate *int, influx *client.Client) {
	fmt.Printf("Sending %d requests\n", *rate)
	go writePoints(influx, *rate)
}

func setupInflux() (influx *client.Client, err error) {
	u, err := url.Parse(fmt.Sprintf("http://%s:%s", os.Getenv("INFLUX_HOST"), os.Getenv("INFLUX_PORT")))
	if err != nil {
		panic(err)
	}

	conf := client.Config{
		URL:      *u,
		Username: os.Getenv("INFLUX_USER"),
		Password: os.Getenv("INFLUX_PWD"),
	}

	con, err := client.NewClient(conf)
	if err != nil {
		panic(err)
	}

	_, _, err = con.Ping()
	if err != nil {
		panic(err)
	}

	return con, err
}

func writePoints(con *client.Client, samples int) {
	var (
		shapes     = []string{"circle", "rectangle", "square", "triangle"}
		colors     = []string{"red", "blue", "green", "black", "purple", "magenta", "pink", "maroon"}
		sampleSize = samples
		pts        = make([]client.Point, sampleSize)
	)

	rand.Seed(42)
	for i := 0; i < sampleSize; i++ {
		pts[i] = client.Point{
			Measurement: "shapes",
			Tags: map[string]string{
				"color": strconv.Itoa(rand.Intn(len(colors))),
				"shape": strconv.Itoa(rand.Intn(len(shapes))),
			},
			Fields: map[string]interface{}{
				"value": rand.Intn(100000),
			},
			Time: time.Now(),
			Precision: "s",
		}
	}

	bps := client.BatchPoints{
		Points:          pts,
		Database:        MyDB,
		RetentionPolicy: "default",
	}
	resp, err := con.Write(bps)
	if err != nil {
		panic(err)
	}

	if resp != nil {
		fmt.Printf("Wrote %d points\n", len(resp.Results))
	}

}