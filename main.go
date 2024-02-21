package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// Incoming Request
type Request struct {
	Ev  string                 `json:"ev"`
	Et  string                 `json:"et"`
	Id  string                 `json:"id"`
	Uid string                 `json:"uid"`
	Mid string                 `json:"mid"`
	T   string                 `json:"t"`
	P   string                 `json:"p"`
	L   string                 `json:"l"`
	Sc  string                 `json:"sc"`
	Atr map[string]interface{} `json:"attributes"`
	Uat map[string]interface{} `json:"traits"`
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

	// Start the HTTP Server to recive requests
	go StartHTTPServer(requests)

	// Create a worker with 5 workers
	for i := 0; i < 5; i++ {
		go worker(requests)
	}

	// Wait
	select {}

}

func StartHTTPServer(requests chan<- Request) {
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

func worker(requests <-chan Request) {
	for req := range requests {
		// Convert the request into Response format
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

		// Convert Attributes
		for key, value := range req.Atr {

			// Check if key starts with "atrk" to parse attributes
			if strings.HasPrefix(key, "atrk") {
				attrKey := strings.Replace(key, "atrk", "", 1)
				attrTypeKey := "atrt" + attrKey
				attrType, ok := req.Atr[attrTypeKey].(string)
				if !ok {
					fmt.Println("Invalid attribute type key")
					continue
				}
				res.Attributes[attrKey] = Details{
					Value: value,
					Type:  attrType,
				}
			}
		}

		// Convert Traits
		for key, value := range req.Uat {
			res.Traits[key] = Details{
				Value: value,
				Type:  getType(value),
			}
		}
		// Send the response to Webhook site
		err := sendToWebhookSite(res)
		if err != nil {
			fmt.Println("Error Sending to webhook site", err)
			return
		}

		fmt.Println("Request Processed and sent to webhook successfully")
	}

}

func getType(value interface{}) string {

	switch value.(type) {
	case int, int32, int64, uint, uint16, uint32, uint8, float32, float64:
		return "number"
	case bool:
		return "boolean"
	default:
		return "string"
	}
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
