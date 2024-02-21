package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Incoming Request
type Request struct {
	Ev  string            `json:"ev"`
	Et  string            `json:"et"`
	Id  string            `json:"id"`
	Uid string            `json:"uid"`
	Mid string            `json:"mid"`
	T   string            `json:"t"`
	P   string            `json:"p"`
	L   string            `json:"l"`
	Sc  string            `json:"sc"`
	Atr map[string]string `json:"-"`
	Uat map[string]string `json:"-"`
}

// UnmarshalJSON
func (r *Request) UnmarshalJSON(data []byte) error {
	type TempRequest struct {
		Ev  string            `json:"ev"`
		Et  string            `json:"et"`
		Id  string            `json:"id"`
		Uid string            `json:"uid"`
		Mid string            `json:"mid"`
		T   string            `json:"t"`
		P   string            `json:"p"`
		L   string            `json:"l"`
		Sc  string            `json:"sc"`
		Atr map[string]string `json:"-"`
		Uat map[string]string `json:"-"`
	}

	// Unmarshal the JSON into the temporary struct
	var temp TempRequest
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	// Copy the fields from the temporary struct to the main struct
	*r = Request(temp)

	// Extract atr and uat fields
	var m map[string]interface{}
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	atr := make(map[string]string)
	uat := make(map[string]string)

	for key, value := range m {
		if len(key) >= 4 && key[:4] == "atrk" {
			index := key[4:]
			atr[value.(string)] = m["atrv"+index].(string)
		} else if len(key) >= 5 && key[:5] == "uatrk" {
			index := key[5:]
			uat[value.(string)] = m["uatrv"+index].(string)
		}
	}

	r.Atr = atr
	r.Uat = uat

	return nil
}

// Response Structure
type Response struct {
	Event      string             `json:"event"`
	EventType  string             `json:"event_type"`
	AppID      string             `json:"app_id"`
	UserID     string             `json:"user_id"`
	MessageID  string             `json:"message_id"`
	PageTitle  string             `json:"page_title"`
	PageURL    string             `json:"page_url"`
	Language   string             `json:"browser_language"`
	Screen     string             `json:"screen_size"`
	Attributes map[string]Details `json:"attributes"`
	Traits     map[string]Details `json:"traits"`
}

// Details of an attribute or trait
type Details struct {
	Value interface{} `json:"value"`
	Type  string      `json:"type"`
}

func main() {
	// Channel to send requests
	requests := make(chan Request)

	var wg sync.WaitGroup

	// Start the HTTP Server to receive requests
	go StartHTTPServer(requests, &wg)

	workerCount := 3
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go worker(requests, &wg)
	}

	wg.Wait()

	fmt.Println("All Go Routine  Workers have Finished")

}

func StartHTTPServer(requests chan<- Request, wg *sync.WaitGroup) {
	defer wg.Done()

	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		decoder := json.NewDecoder(r.Body)
		var req Request

		err := decoder.Decode(&req)
		if err != nil {
			http.Error(w, "Invalid Request Format", http.StatusBadRequest)
			return
		}

		// Send the request to the channel
		requests <- req

		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "Request Received and sent to worker")
	})
	fmt.Println("Server running on port 0808")
	http.ListenAndServe(":0808", nil)

}

func worker(requests <-chan Request, wg *sync.WaitGroup) {
	defer wg.Done()

	for req := range requests {
		// Convert the request into Response format
		res := convertToResponse(req)

		// Send the response to Webhook site
		err := sendToWebhookSite(res)
		if err != nil {
			fmt.Println("Error Sending to webhook site", err)
			return
		}

		fmt.Println("Request Processed and sent to webhook successfully")
	}

}

func convertToResponse(req Request) Response {
	res := Response{
		Event:      req.Ev,
		EventType:  req.Et,
		AppID:      req.Id,
		UserID:     req.Uid,
		MessageID:  req.Mid,
		PageTitle:  req.T,
		PageURL:    req.P,
		Language:   req.L,
		Screen:     req.Sc,
		Attributes: make(map[string]Details),
		Traits:     make(map[string]Details),
	}

	for key, value := range req.Atr {
		res.Attributes[key] = Details{
			Value: value,
			Type:  getType(value),
		}
	}
	for key, value := range req.Uat {
		res.Traits[key] = Details{
			Value: value,
			Type:  getType(value),
		}
	}

	return res
}

// getType returns the type of the value
func getType(value string) string {

	// Remove  double quotes
	trimmedValue := strings.Trim(value, `"`)

	// Check if  value is a number
	if _, err := strconv.ParseFloat(trimmedValue, 64); err == nil {
		return "number"
	}

	// Check if value represents is a boolean
	if _, err := strconv.ParseBool(trimmedValue); err == nil {
		return "boolean"
	}

	return "string"
}

func sendToWebhookSite(res Response) error {

	payload, err := json.Marshal(res)
	if err != nil {
		return err
	}

	// Send Payload to webhook endpoint
	_, err = http.Post("https://webhook.site/282ce484-d129-4991-9200-79cd0f3c78d6", "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return err
	}

	return nil
}
