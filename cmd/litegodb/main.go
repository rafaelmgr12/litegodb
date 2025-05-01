package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

func main() {
	// CLI flag: --url
	url := flag.String("url", "http://localhost:8080", "LiteGoDB server URL")
	flag.Parse()

	fmt.Println("Connected to LiteGoDB at", *url)
	fmt.Println("Type SQL queries or 'exit' to quit.")

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}
		query := strings.TrimSpace(line)

		if query == "" {
			continue
		}
		if strings.ToLower(query) == "exit" {
			break
		}

		resp, err := sendSQL(*url, query)
		if err != nil {
			fmt.Println("❌", err)
			continue
		}

		fmt.Println(resp)
	}
}

func sendSQL(url, query string) (string, error) {
	reqBody := map[string]string{"query": query}
	bodyBytes, _ := json.Marshal(reqBody)

	resp, err := http.Post(url+"/sql", "application/json", bytes.NewBuffer(bodyBytes))
	if err != nil {
		return "", fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server error: %s", string(body))
	}

	return string(body), nil
}
