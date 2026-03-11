package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

func main() {
	log.SetFlags(0)

	usernamePtr := flag.String("u", "", "Username for authentication")
	passwordPtr := flag.String("p", "", "Password for authentication")
	// MEJORA: Nueva bandera para flexibilizar la ubicación del certificado TLS
	certPathPtr := flag.String("cert", "certificates/server.crt", "Path to the server's TLS certificate")
	flag.Parse()

	addr := "localhost:5876"
	if flag.NArg() > 0 {
		addr = flag.Arg(0)
	}

	if !strings.Contains(addr, ":") {
		log.Fatal(colorErr("Error: The server address must be in the format 'host:port'. Provided: ", addr))
	}

	// TLS Connection Configuration
	fmt.Println(colorInfo("Connecting to LunaDB server at ", addr))
	caCert, err := os.ReadFile(*certPathPtr)
	if err != nil {
		log.Fatal(colorErr(fmt.Sprintf("Failed to read server certificate '%s': %v", *certPathPtr, err)))
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs:    caCertPool,
		ServerName: strings.Split(addr, ":")[0],
	}

	// Connect using TLS
	conn, err := tls.Dial("tcp", addr, tlsConfig)
	if err != nil {
		log.Fatal(colorErr("Failed to connect via TLS to %s: %v", addr, err))
	}
	defer conn.Close()

	fmt.Println(colorOK("√ Connected securely."))

	// Initialize and run the client
	client := newCLI(conn)
	if err := client.run(usernamePtr, passwordPtr); err != nil {
		log.Fatal(colorErr("Client error: %v", err))
	}
}
