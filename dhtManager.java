
import java.io.*;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Collections;

public class dhtManager
{

    private static ArrayList<String> ports = new ArrayList<String>();
    private static ArrayList<Integer> ids = new ArrayList<Integer>();
    private static ArrayList<Integer> idsSorted = new ArrayList<Integer>();
    public final static int FILE_SIZE = 6022386;

    private static final int M =5;
    static Socket s = null;
    static String IP = "172.28.17.219";

    public static void main(String args[])
    {
        while(true) {

            Scanner in = new Scanner(System.in);

            System.out.println("---------------------------------");

            System.out.println("1 : Add a new node");
            System.out.println("2 : Remove a node");
            System.out.println("3 : Add a new file");
            System.out.println("4 : Search a file");

            System.out.println("---------------------------------");
            System.out.print("Please choose one of the above option : ");

            int option = in.nextInt();
            switch(option)
            {
                case 1 : addANewNode();
                break;
                case 2 : removeANode();
                break;
                case 3 : addAFile();
                break;
                case 4 : searchAffile();
                break;
                default:break;
            }

        }
    }

    public static void addANewNode()
    {
        //add the node ipaddress and port, hash it and get the id and ask the nodes to update the finger table
        String hash = "";

        System.out.print("enter the address of the node (ipaddress:port) : ");
        Scanner in = new Scanner(System.in);
        String port  = in.nextLine();
        double idDouble = 0;
        int id = 0;



        try
        {
            String info[] = port.split(":");
            hash = sha1("localhost:"+info[1]);

            String idString  = hash.substring(0,2);
            idDouble = Integer.parseInt(idString, 16);
            idDouble = idDouble % Math.pow(2, M);
            id = (int) idDouble;

            while(ids.contains(id))
            {
                System.out.println("The id already exists . Please change the address : ");
                Scanner in1 = new Scanner(System.in);
                port = in.nextLine();

                info = port.split(":");
                hash = sha1("localhost:"+info[1]);

                idString  = hash.substring(0,2);
                idDouble = Integer.parseInt(idString, 16);
                idDouble = idDouble % Math.pow(2, M);
                id = (int) idDouble;

            }

            ids.add(id);
            ports.add(port);
            idsSorted.add(id);

            Collections.sort(idsSorted);

            String fingerTable = "";

            for(int i =1; i < M + 1; i++)
            {
                double key = id + Math.pow(2, i-1);
                int j ;
                for(j=0; j<ids.size() - 1; j++)
                {
                    if(key == idsSorted.get(j))
                    {
                        fingerTable = fingerTable + idsSorted.get(j) + '/' + ports.get(ids.indexOf(idsSorted.get(j)));
                    }

                    if(key > idsSorted.get(j) && key <= idsSorted.get(j+1))
                    {
                        fingerTable = fingerTable + idsSorted.get(j+1) + '/' + ports.get(ids.indexOf(idsSorted.get(j+1)));
                    }
                }

                if(ids.size() > 1) {
                    if (key > idsSorted.get(j) || (key > 0 && key < idsSorted.get(0))) {
                        fingerTable = fingerTable + idsSorted.get(0) + '/' + ports.get(ids.indexOf(idsSorted.get(0)));
                    }
                }

                fingerTable = fingerTable + "//";
            }

            String address[] = port.split(":");
            s = new Socket(address[0], Integer.parseInt(address[1]));

            PrintWriter out = new PrintWriter(s.getOutputStream(), true);

            int successorid, predecessorid;
            String successorport, predecessorport;


            System.out.println("Node ID added: " + id);
            //System.out.println(idsSorted.indexOf(id));

            if(idsSorted.size() == 1) // first initialized node
            {
                successorid = id;
                successorport = port;
                predecessorid = id;
                predecessorport = port;
            }

            else if(idsSorted.indexOf(id) == 0) // node is the first one
            {
                successorid = idsSorted.get( idsSorted.indexOf(id) + 1);
                predecessorid = idsSorted.get(idsSorted.size() - 1);
                successorport = ports.get(ids.indexOf(successorid));
                predecessorport = ports.get(ids.indexOf(predecessorid));

            }

            else if(idsSorted.indexOf(id) == ( idsSorted.size() - 1 ) ) // node is the last one
            {
                successorid = idsSorted.get(0);
                predecessorid = idsSorted.get( idsSorted.indexOf(id) - 1);
                successorport = ports.get(ids.indexOf(successorid));
                predecessorport = ports.get(ids.indexOf(predecessorid));

            }

            else
            {
                successorid = idsSorted.get( idsSorted.indexOf(id) + 1);
                predecessorid = idsSorted.get( idsSorted.indexOf(id) - 1);
                successorport = ports.get(ids.indexOf(successorid));
                predecessorport = ports.get(ids.indexOf(predecessorid));

            }

            out.println("initialize" + "," + (int)id + "," + successorid + "," + successorport + ","  +  predecessorid + "," + predecessorport + "," +fingerTable);


        }
        catch (IOException e)
        {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }


        for(int i=0; i<ports.size() - 1; i++)
        {
            try
            {
                //System.out.println("the port updating .... " + ports.get(i));
                String address[] = ports.get(i).split(":");
                s = new Socket(address[0], Integer.parseInt(address[1]));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println("newNode :" + port + ":" + id);
            }

            catch(Exception e)
            {
                e.printStackTrace();
            }

            finally {
                try {
                    s.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }







    }

    public static void removeANode()
    {
        //remove the node , hash its ipaddress and port to get the id and ask the nodes to update the finger table
        String hash = "";
        double idDouble;
        int id;

        System.out.print("Enter the node address to be removed (ipaddress:port) : ");
        Scanner in = new Scanner(System.in) ;
        String port = in.nextLine();

        try {
            String info[] = port.split(":");
            hash = sha1("localhost:"+info[1]);
            String idString = hash.substring(0, 2);
            idDouble = Integer.parseInt(idString, 16);
            idDouble = idDouble % Math.pow(2, M);
            id = (int)idDouble;

            System.out.println("Node ID removed: " + id);

            int successorID, precedessorID;
            String successorPort, precedessorPORT;

            if(idsSorted.indexOf(id) == idsSorted.size() - 1) //if node is the last one
            {
                successorID = idsSorted.get(0);
                successorPort = ports.get(ids.indexOf(successorID));
            }

            else
            {
                successorID = idsSorted.get(idsSorted.indexOf(id) + 1);
                successorPort= ports.get(ids.indexOf(successorID));
            }

            if(idsSorted.indexOf(id) == 0) //if node is the first node
            {
                precedessorID = idsSorted.get(idsSorted.size() - 1);
                precedessorPORT = ports.get(ids.indexOf(precedessorID));
            }

            else
            {
                precedessorID = idsSorted.get(idsSorted.indexOf(id) - 1);
                precedessorPORT = ports.get(ids.indexOf(precedessorID));
            }

            ids.remove(ids.indexOf(id));
            ports.remove(ports.indexOf(port));
            idsSorted.remove(idsSorted.indexOf(id));



            for(int i=0; i<ports.size(); i++)
            {
                String address[] = ports.get(i).split(":");
                s = new Socket(address[0], Integer.parseInt(address[1]));
                PrintWriter out = new PrintWriter(s.getOutputStream(), true);
                out.println("removeNode:" + (int)id + ":" + port + ":" + successorID + ":" + successorPort + ":" + precedessorID + ":" + precedessorPORT);
            }

        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

    }

    public static void searchAffile()
    {
        //hash the file name and get the id,
        //start the search at a particular node

        Scanner in = new Scanner(System.in);
        System.out.print("Enter the file name to be searched: ");
        String filename = in.nextLine();

        String home = System.getProperty("user.home");
        //System.out.println("User home directory is: " + home);
        File userHome = new File(home);
        File myFile = new File(userHome, filename + ".Found");

        String hash = null;
        try {
            hash = sha1(filename);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String idString  = hash.substring(0,2);
        double id = Integer.parseInt(idString, 16);
        id = id % Math.pow(2, M);

        try {
            String address[] = ports.get(0).split(":");
            s = new Socket(address[0], Integer.parseInt(address[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);
            out.println( "findFile" + ":" + (int)id + ":" + filename);
            out.flush();
            byte[] mybytearray = new byte[FILE_SIZE];
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(myFile)); //output stream of the file
            InputStream is = s.getInputStream(); // input stream of the socket
            int bytesRead = is.read(mybytearray, 0, mybytearray.length); // read file from socket
            if(bytesRead == -1)
                System.out.println("error reading file " + filename);
            else {
                //System.out.println(bytesRead);
                System.out.println("File found and stored as " + filename + ".Found " + " in the home directory");
                bos.write(mybytearray, 0, bytesRead); // write to the file
                bos.close();
                s.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void addAFile()
    {
        //hash the file name and get the id,
        //start the insertion at a particular node


        Scanner in = new Scanner(System.in);
        System.out.print("Enter the file name to be added : ");
        String filename = in.nextLine();

        String hash = null;
        try {
            hash = sha1(filename);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        String idString  = hash.substring(0,2);
        double id = Integer.parseInt(idString, 16);
        id = id % Math.pow(2, M);

        String home = System.getProperty("user.home");
        File userHome = new File(home);
        File myFile = new File(userHome, filename);

        try {
            String address[] = ports.get(0).split(":");
            s = new Socket(address[0], Integer.parseInt(address[1]));
            PrintWriter out = new PrintWriter(s.getOutputStream(), true);

            out.println( "newFile" + ":" + (int)id + ":" + filename);
            out.flush();
            byte[] mybytearray = new byte[(int) myFile.length()];

            BufferedInputStream bis = new BufferedInputStream(new FileInputStream(myFile));
            if (bis.read(mybytearray, 0, mybytearray.length) == -1 )
                System.out.println("error reading file " + filename);
            else {
                System.out.println("File " + filename + " with id " + (int)id + " has been added to the chord");
                BufferedOutputStream bos = new BufferedOutputStream(s.getOutputStream()); //output stream of the file
                bos.write(mybytearray, 0, mybytearray.length);
                bos.flush();
                bos.close();

            }

            bis.close();
            s.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static String sha1(String input) throws NoSuchAlgorithmException
    {
        MessageDigest mDigest = MessageDigest.getInstance("SHA1");
        byte[] result = mDigest.digest(input.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++)
        {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }

        return sb.toString();
    }

    public static String check_ip(String ip)
    {
        if(ip == IP || ip.equals("127.0.0.1"))
            return "127.0.0.1";
        else
            return ip;
    }

}
