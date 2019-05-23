//localhost:9991
//localhost:9992
//localhost:9993
//localhost:9994

// TODO - remove and add a node after adding a file

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;

public class node4
{

    private final static int M = 5;
    private final static int PORT = 9994;
    private final static String IP = "127.0.0.1";
    private static int ID;
    public static class node
    {
        public int id;
        public String ipaddress;
        public int port;
    };

    private static node[] fingerTable = new node[M];
    private static node successor, predeccesor;

    private static ArrayList<Integer> fileIDs = new  ArrayList<Integer>();
    private static ArrayList<String> files = new  ArrayList<String>();
    public final static int FILE_SIZE = 6022386;

    public static void main(String args[])
    {
        try {
            ServerSocket server = new ServerSocket(PORT);
            Socket dhtManager = new Socket();
            dhtManager = server.accept();
            PrintWriter out =
                    new PrintWriter(dhtManager.getOutputStream(), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(dhtManager.getInputStream()));
            String response = in.readLine();

            while(response != null)
            {
                if(response.contains("initialize"))
                    initiazeNode(response);

                if(response.contains("newNode"))
                    updateFingerTable(response);

                if(response.contains("removeNode"))
                    removeNode(response);

                if(response.contains("giveFiles"))
                {
                    for(int i=0; i<fileIDs.size(); i++)
                    {
                        successor.ipaddress = check_ip(successor.ipaddress);
                        Socket s = new Socket(successor.ipaddress, successor.port);
                        out = new PrintWriter(s.getOutputStream(), true);
                        out.println( "newFile" + ":" + fileIDs.get(i) + ":" + files.get(i));
                        out.flush();
                        sendFile(files.get(i), s);
                        s.close();

                    }
                }


                if(response.contains("newFile"))
                {
                    // route using fingertable
                    String information[] = response.split(":"); //get all the information
                    int id = Integer.parseInt(information[1]); //id
                    String name = information[2]; //name of the file

                    clear();
                    System.out.println("New File " + name + " with id " + id + " has been recieved");


                    int port = -1;

                    if( (id <= ID && id > predeccesor.id)|| (predeccesor.id > ID && (id <= ID || id > predeccesor.id) )  || successor.id == ID || predeccesor.id == ID) //if the node is suppose to store the file
                    {
                        storeFile(id, name, dhtManager); // store the file coming in from the same socket
                    }

                    else if( (id > ID && id <= successor.id) || (successor.id <= ID && (id >= ID || id <= successor.id ) ) )
                    {
                        try
                        {
                            successor.ipaddress = check_ip(successor.ipaddress);
                            Socket s = new Socket(successor.ipaddress, successor.port);
                            PrintWriter output = new PrintWriter(s.getOutputStream(), true); // let the successor know about new file
                            output.println(response);
                            output.flush();
                            byte[] mybytearray = new byte[FILE_SIZE];
                            BufferedOutputStream bos = new BufferedOutputStream(s.getOutputStream()); //output stream of the file
                            BufferedInputStream bis = new BufferedInputStream(dhtManager.getInputStream());
                            int bytesRead = -1;
                            bytesRead = bis.read(mybytearray);  // read the file from successor
                            if(bytesRead == -1)
                            {
                                System.out.println("error encountered while reading the file");
                            }
                            else {
                                bos.write(mybytearray, 0, bytesRead); // write to the socket
                                System.out.println("file sent to the successor : " + successor.id);
                            }
                            bos.close();
                            bis.close();
                            dhtManager.close();
                            s.close();

                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }

                    else
                    {
                        int idFile = -1;
                        int portFile = -1;
                        String ip = "";
                        for(int i=0; i< M; i++)
                        {
                            if(id >= fingerTable[i].id && fingerTable[i].id >= idFile)
                            {
                                idFile = fingerTable[i].id;
                                portFile = fingerTable[i].port;
                                ip = fingerTable[i].ipaddress;
                            }
                        }

                        if(idFile == -1) {
                            try
                            {
                                fingerTable[0].ipaddress = check_ip(fingerTable[0].ipaddress);
                                Socket s = new Socket(fingerTable[0].ipaddress, fingerTable[0].port);
                                PrintWriter output = new PrintWriter(s.getOutputStream(), true);
                                output.println(response);
                                output.flush();
                                byte[] mybytearray = new byte[FILE_SIZE];
                                BufferedOutputStream bos = new BufferedOutputStream(s.getOutputStream()); //output stream of the file
                                BufferedInputStream bis = new BufferedInputStream(dhtManager.getInputStream());
                                int bytesRead = bis.read(mybytearray);  // read the file from successor
                                if(bytesRead == -1)
                                {
                                    System.out.println("error encountered while reading the file");
                                }
                                else {
                                    bos.write(mybytearray, 0, bytesRead); // write to the socket
                                    System.out.println(name + " sent to the first node in the table : " + fingerTable[0].id);
                                }
                                bos.close();
                                bis.close();
                                dhtManager.close();
                                s.close();
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();
                            }
                        }
                        else
                        {
                            try
                            {
                                ip = check_ip(ip);
                                Socket s = new Socket(ip, portFile);
                                PrintWriter output = new PrintWriter(s.getOutputStream(), true);
                                output.println(response);
                                output.flush();
                                byte[] mybytearray = new byte[FILE_SIZE];
                                BufferedOutputStream bos = new BufferedOutputStream(s.getOutputStream()); //output stream of the file
                                BufferedInputStream bis = new BufferedInputStream(dhtManager.getInputStream());
                                int bytesRead = bis.read(mybytearray);  // read the file from successor
                                if(bytesRead == -1)
                                {
                                    System.out.println("error encountered while reading the file");
                                }
                                else {
                                    bos.write(mybytearray, 0, bytesRead); // write to the socket
                                    System.out.println( name + " sent to the closest predecessor : " + idFile);
                                }
                                bos.close();
                                bis.close();
                                dhtManager.close();
                                s.close();
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();;
                            }
                        }

                    }

                }

                if(response.contains("findFile"))
                {
                    // route using fingertable
                    clear();
                    String information[] = response.split(":");
                    int id = Integer.parseInt(information[1]);
                    String name = information[2];

                    int port = -1;

                    if( (id <= ID && id > predeccesor.id)|| (predeccesor.id > ID && (id <= ID || id > predeccesor.id) ) || ID == predeccesor.id ) // if the file is between predecessor and ID
                    {
                        sendFile(name, dhtManager);
                        System.out.println( name + " has been found here and sent");
                    }

                    else if( (id > ID && id <= successor.id) || (successor.id <= ID && (id >= ID || id <= successor.id ) ) )
                    {
                        try
                        {
                            successor.ipaddress = check_ip(successor.ipaddress);
                            Socket s = new Socket(successor.ipaddress, successor.port);
                            PrintWriter output = new PrintWriter(s.getOutputStream(), true); //send findfile to successor
                            output.println(response);
                            System.out.println("request for the file " + name + " has been passed to the successor : " + successor.id);
                            output.flush();
                            byte[] mybytearray = new byte[FILE_SIZE];
                            BufferedOutputStream bos = new BufferedOutputStream(dhtManager.getOutputStream()); //output stream of the file
                            BufferedInputStream bis = new BufferedInputStream(s.getInputStream());
                            int bytesRead = bis.read(mybytearray);  // read the file from successor
                            if(bytesRead == -1)
                            {
                                System.out.println("error encountered while reading the file");
                            }
                            else {
                                bos.write(mybytearray, 0, bytesRead); // write to the socket
                                System.out.println(name + " received from the node : " + successor.id);
                            }
                            bos.close();
                            bis.close();
                            dhtManager.close();
                            s.close();
                        }
                        catch (Exception e)
                        {
                            e.printStackTrace();
                        }
                    }

                    else
                    {
                        int idFile = -1;
                        int portFile = -1;
                        String ip = "";

                        for(int i=0; i< M; i++)
                        {
                            if(id >= fingerTable[i].id && fingerTable[i].id >= idFile)
                            {
                                idFile = fingerTable[i].id;
                                portFile = fingerTable[i].port;
                                ip = fingerTable[i].ipaddress;
                            }
                        }

                        if(idFile == -1) {
                            try
                            {
                                fingerTable[0].ipaddress = check_ip(fingerTable[0].ipaddress);
                                Socket s = new Socket(fingerTable[0].ipaddress, fingerTable[0].port);
                                PrintWriter output = new PrintWriter(s.getOutputStream(), true); //send findfile to successor
                                output.println(response);
                                System.out.println("request for the file " + name + " has been passed to the first node in the table : " + fingerTable[0].id);
                                output.flush();
                                byte[] mybytearray = new byte[FILE_SIZE];
                                BufferedOutputStream bos = new BufferedOutputStream(dhtManager.getOutputStream()); //output stream of the file
                                BufferedInputStream bis = new BufferedInputStream(s.getInputStream());
                                int bytesRead = bis.read(mybytearray);  // read the file from successor
                                if(bytesRead == -1)
                                {
                                    System.out.println("error encountered while reading the file");
                                }
                                else {
                                    bos.write(mybytearray, 0, bytesRead); // write to the socket

                                    System.out.println(name + " received from the node : " + fingerTable[0].id);
                                }
                                bos.close();
                                bis.close();
                                dhtManager.close();
                                s.close();
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();
                            }
                        }
                        else
                        {
                            try
                            {
                                ip = check_ip(ip);
                                Socket s = new Socket(ip, portFile);
                                PrintWriter output = new PrintWriter(s.getOutputStream(), true); //send findfile to successor
                                output.println(response);
                                System.out.println("request for the file " + name + " has been passed to the node : " + idFile);
                                output.flush();;
                                byte[] mybytearray = new byte[FILE_SIZE];
                                BufferedOutputStream bos = new BufferedOutputStream(dhtManager.getOutputStream()); //output stream of the file
                                BufferedInputStream bis = new BufferedInputStream(s.getInputStream());
                                int bytesRead = bis.read(mybytearray);  // read the file from successor
                                if(bytesRead == -1)
                                {
                                    System.out.println("error reading the file and sending it ");
                                }
                                else {
                                    bos.write(mybytearray, 0, bytesRead); // write to the socket
                                    System.out.println(name + " received from the node : " + idFile);
                                }
                                bos.close();
                                bis.close();
                                dhtManager.close();
                                s.close();
                            }
                            catch (Exception e)
                            {
                                e.printStackTrace();;
                            }
                        }

                    }



                }

                dhtManager.close();
                dhtManager = server.accept();
                out = new PrintWriter(dhtManager.getOutputStream(), true);
                in = new BufferedReader(new InputStreamReader(dhtManager.getInputStream()));
                response = in.readLine();
                out.flush();

            }

        } catch (IOException e) {
            //e.printStackTrace();
        }

    }

    public static void updateFingerTable(String message)
    {
        String information[] = message.split(":");
        String ip = information[1];
        int port = Integer.parseInt(information[2]);
        int id = Integer.parseInt(information[3]);



        if(successor.id == ID)
        {
            successor.id = id;
            successor.port = port;
            successor.ipaddress = ip;

            predeccesor.id = id;
            predeccesor.port = port;
            predeccesor.ipaddress = ip;
        }

        else
        {
            ArrayList<Integer> sorted = new ArrayList<Integer>();
            sorted.add(successor.id);
            sorted.add(predeccesor.id);
            sorted.add(id);
            sorted.add(ID);

            Collections.sort(sorted);
            int pid = predeccesor.id;
            int sid = successor.id;

            if (sorted.indexOf(ID) == 0)
            {
                predeccesor.id = sorted.get(3);
                successor.id = sorted.get(1);
            }

            else if (sorted.indexOf(ID) == 3)
            {
                predeccesor.id = sorted.get(2);
                successor.id = sorted.get(0);
            }

            else
            {
                predeccesor.id = sorted.get(sorted.indexOf(ID) - 1);
                successor.id = sorted.get(sorted.indexOf(ID) + 1);
            }

            if (successor.id == id) {
                successor.port = port;
                successor.ipaddress = ip;
            }
            if (predeccesor.id == id)
            {
                predeccesor.port = port;
                predeccesor.ipaddress = ip;

            }
        }

        if (predeccesor.id == id)
        {

            for(int i=0; i<fileIDs.size(); i++)
            {
                if( !(fileIDs.get(i) <= ID && fileIDs.get(i) > id) || fileIDs.get(i) <= id)
                {
                    try
                    {
                        predeccesor.ipaddress = check_ip(predeccesor.ipaddress);
                        Socket s = new Socket(predeccesor.ipaddress, predeccesor.port);
                        PrintWriter out =
                                new PrintWriter(s.getOutputStream(), true);
                        out.println("newFile" + ":" + fileIDs.get(i) + ":" + files.get(i));
                        System.out.println("sending file " + files.get(i) + " to the new node with id : "+ predeccesor.id);
                        out.flush();
                        sendFile(files.get(i), s);
                        files.remove(i);
                        fileIDs.remove(i);

                        i--;
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
            }

        }







        for(int i=1; i<M +1 ; i++)
        {

            double key = ID + Math.pow(2, i - 1);
            key = key % Math.pow(2, M);
            if((key > fingerTable[i-1].id || key <= id) && id < fingerTable[i-1].id) // when a node is added before the first one
            {
                fingerTable[i-1].id = id;
                fingerTable[i-1].port = port;
                fingerTable[i-1].ipaddress = ip;

            }
            if(key > fingerTable[i-1].id && key <= id)  // when a node is added after the last one
            {
                fingerTable[i-1].id = id;
                fingerTable[i-1].port = port;
                fingerTable[i-1].ipaddress = ip;
            }

            if(key < id && id < fingerTable[i-1].id) // if its in between
            {
                fingerTable[i-1].id = id;
                fingerTable[i-1].port = port;
                fingerTable[i-1].ipaddress = ip;
            }

        }

        printFingerTable();
        System.out.println("Message From Manager : new node " + id + " has been added to the chord");
    }

    public static void initiazeNode(String message)
    {
        successor = new node();
        predeccesor = new node();

        String information[];
        String table[];
        information = message.split(",");
        //information[0] will be the action to be performed
        ID = Integer.parseInt(information[1]); // the id received from the manager
        successor.id =  Integer.parseInt(information[2]);
        successor.ipaddress = information[3].split(":")[0];
        successor.port = Integer.parseInt(information[3].split(":")[1]);
        predeccesor.id =  Integer.parseInt(information[4]);
        predeccesor.ipaddress = information[5].split(":")[0];
        predeccesor.port = Integer.parseInt(information[5].split(":")[1]);



        if(successor.id == ID)
        {
            for(int i =0; i < M; i++)
            {
                fingerTable[i] = new node();
                fingerTable[i].id = ID;
                fingerTable[i].port = PORT;
                fingerTable[i].ipaddress = IP;
            }
        }

        else {
            table = information[6].split("//");

            for (int i = 0; i < M; i++) {
                String value[] = table[i].split("/");
                fingerTable[i] = new node();
                fingerTable[i].id = Integer.parseInt(value[0]);
                String address[] = value[1].split(":");
                fingerTable[i].ipaddress = address[0];
                fingerTable[i].port = Integer.parseInt(address[1]);
            }
        }

        printFingerTable();
    }

    public static void removeNode(String message)
    {

        String information[] = message.split(":");

        int id = Integer.parseInt(information[1]);
        String idip = information[2];
        int idPort = Integer.parseInt(information[3]);


        int successorid = Integer.parseInt(information[4]);
        String successorip = information[5];
        int successorPort = Integer.parseInt(information[6]);

        int predecessorid = Integer.parseInt(information[7]);
        String predecessorip = information[8];
        int predecessorport = Integer.parseInt(information[9]);

        if(successor.id == id) {
            successor.id = successorid;
            successor.port = successorPort;
            successor.ipaddress = successorip;
        }


        if(id == predeccesor.id)
        {
            predeccesor.id = predecessorid;
            predeccesor.port = predecessorport;
            predeccesor.ipaddress = predecessorip;

            Socket s = null;
            try {
                idip = check_ip(idip);
                s = new Socket(idip, idPort);
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                System.out.println("Files belonging to node " + id + " has been requested to be stored here");
                out.println("giveFiles");
                out.close();
            }
            catch (IOException e)
            {
                System.out.println("node " +idip+ " is offline");

            } finally {
                try {
                    if(s != null)
                        s.close();
                } catch (IOException e) {
                    System.out.println("node " +idip+ " is offline");
                }
            }


        }

        for(int i = 0; i<M; i++)
        {
            if(fingerTable[i].id == id)
            {
                fingerTable[i].id = successorid;
                fingerTable[i].ipaddress = successorip;
                fingerTable[i].port = successorPort;
            }
        }

        printFingerTable();
        System.out.println("Message From Manager : node " + id + " has been removed");
    }

    public static void printFingerTable()
    {
        clear();
        System.out.println("NODE ID        : " + ID);
        System.out.println("SUCCESSOR ID   : " + successor.id);
        System.out.println("PREDECCESOR ID : " + predeccesor.id);
        System.out.println("-----------------------");
        System.out.println( "| " + "i" + " | " + "key" + "  | " + "succ(key)" + "| ");
        System.out.println("-----------------------");
        String keyStr, succ;
        for(int i =1; i < M + 1; i++)
        {
            double key = ID + Math.pow(2, i - 1);
            key = key % Math.pow(2, M);

            if(key < 10)
                keyStr = "0" + key;
            else
                keyStr = String.valueOf(key);
            if(fingerTable[i-1].id < 10)
                succ = "0" + fingerTable[i-1].id;
            else
                succ = String.valueOf(fingerTable[i-1].id);

            System.out.println( "| " + i + " | " + keyStr + " |    " + succ + "    | ");

        }
        System.out.println("-----------------------");
    }

    public static void clear()
    {
        System.out.println("----------------------------------------------------------------------------------");
    }

    public static void sendFile(String name, Socket sock)
    {

        String home = System.getProperty("user.home");
        File userHome = new File(home);
        File myFile = new File(userHome, ID + name);

        try
        {
            byte[] mybytearray = new byte[(int) myFile.length()];
            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(myFile));
            BufferedOutputStream bos = new BufferedOutputStream(sock.getOutputStream());
            if ( bis.read(mybytearray, 0, mybytearray.length - 1) == -1 )
            {
                System.out.println("error reading the file " + ID + name);
            }
            else {
                bos.write(mybytearray, 0, mybytearray.length - 1);
                bos.flush();
                System.out.println(name + " has been sent");
            }

            bos.flush();
            bos.close();
            bis.close();
            sock.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void storeFile(int id, String name, Socket sock)
    {
        String home = System.getProperty("user.home");
        File userHome = new File(home);
        File myFile = new File(userHome, ID + name);

        try
        {
            byte[] mybytearray = new byte[FILE_SIZE];
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(myFile)); //output stream of the file
            InputStream bis = sock.getInputStream();
            int bytesRead = bis.read(mybytearray); // read file from socket
            if(bytesRead == -1)
            {
                System.out.println("error reading the file " + ID + name);
            }
            else {
                bos.write(mybytearray, 0, bytesRead); // write to the file
                fileIDs.add(id);
                files.add(name);
                System.out.println(name + " has been stored here");
            }

            bos.flush();
            bos.close();
            bis.close();
            sock.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String check_ip(String ip)
    {
        if(ip == IP || ip.equals("127.0.0.1"))
            return "127.0.0.1";
        else
            return ip;
    }
}
