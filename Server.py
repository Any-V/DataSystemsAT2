import socket
import sys

def handle_request(connection_socket):
    try:
        # Receive and decode request message
        message = connection_socket.recv(1024).decode()
        
        # Extract requested file (defaulting to index.html)
        endPoint = message.split()[1] if message else "/index.html"
        filename = endPoint.lstrip("/")  # Remove leading slash
        
        # Attempt to open and read file
        try:
            with open(filename, "rb") as f:
                output_data = f.read()
            
            # Send HTTP response headers
            connection_socket.send(b"HTTP/1.1 200 OK\r\n\r\n")
            connection_socket.send(output_data)
        except FileNotFoundError:
            error_message = b"HTTP/1.1 404 Not Found\r\n\r\nFile Not Found"
            connection_socket.send(error_message)
        
        connection_socket.send(b"\r\n")
    except Exception as e:
        print(f"Error processing request: {e}")
    finally:
        connection_socket.close()

# Server setup
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(("", 6789))  # Bind to all available interfaces on port 6789
server_socket.listen(1)

print("Server running... Listening on port 6789")

# Main loop to handle incoming connections
while True:
    connection_socket, addr = server_socket.accept()
    print(f"Connection from {addr}")
    handle_request(connection_socket)

server_socket.close()
sys.exit()