package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

const (
	REQUEST_DELIM = '\n'
)

type ServerConfig struct {
	Host string
	Port int
}

type SocketServer struct {
	Config *ServerConfig
}

func (this *SocketServer) Start() {
	log.Println("== Starting Socket Server ==")
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", this.Config.Port))
	if err != nil {
		log.Fatal("Unable to start the socket server", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("Unable to checkout connection", err)
		}
		go this.handleConnection(conn)
	}
}

func (this *SocketServer) handleConnection(conn net.Conn) {
	defer func() {
		if conn != nil {
			log.Println("Closing the open connection from client", conn.RemoteAddr)
			conn.Close()
		}
	}()

	for {
		var delim byte = byte(REQUEST_DELIM)
		bytes, err := bufio.NewReader(conn).ReadBytes(delim)
		if err != nil {
			log.Println("Error in reading bytes - ignoring - please retry", err)
			break
		}
		log.Println("=>Message received: ", string(bytes[:]))

		outBytes := []byte("hello client!\n")
		conn.Write(outBytes)
		log.Printf("Wrote response bytes to the client: %s", string(outBytes[:]))
	}
}
