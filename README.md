# dhtManager

This project was developed for solving a real-life problem of convenience for the metro users by using DHT chord implementation to find out bus timings at stations. The purpose was to learn how to implement a distributed system and maintain the state using DHT chord.

A DHT peer to peer chord has been implemented to add new files and handle file requests. The reason behind this was to distribute the management of the files among different metro servers. Additionally, whenever a server makes a request for a file, it follows the DHT protocol to look for the file and does not store a copy to serve other requests. This makes it easier to change timings in any one file by one server without taking care of synchronization of data as whenever a request is made from one server, the chord routes the request to the server handling the file. 

A manager acting as a super peer was used to manage adding nodes or removing nodes from the chord. Adding and searching a file to the chord can be done from anywhere by giving the address of one of the node in the chord. The architecture is displayed below. A user application can make request to any of the nodes in the chord and it would get the timings from the same node as once the file is found, the file is forwarded using the route/hops through which the request was made hence ensuring transparency.


