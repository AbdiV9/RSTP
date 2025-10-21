/*
 * Replace the following string of 0s with your student number
 * 000000000
 */
import java.io.File;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.DatagramPacket;


public class Protocol {

    static final String  NORMAL_MODE="nm"   ;         // normal transfer mode: (for Part 1 and 2)
    static final String     TIMEOUT_MODE ="wt"  ;        // timeout transfer mode: (for Part 3)
    static final String     LOST_MODE ="wl"  ;           // lost Ack transfer mode: (for Part 4)
    static final int DEFAULT_TIMEOUT =1000  ;         // default timeout in milliseconds (for Part 3)
    static final int DEFAULT_RETRIES =4  ;            // default number of consecutive retries (for Part 3)
    public static final int MAX_Segment_SIZE = 4096;  //the max segment size that can be used when creating the received packet's buffer

    /*
     * The following attributes control the execution of the transfer protocol and provide access to the
     * resources needed for the transfer
     *
     */

    private InetAddress ipAddress;      // the address of the server to transfer to. This should be a well-formed IP address.
    private int portNumber;           // the  port the server is listening on
    private DatagramSocket socket;      // the socket that the client binds to

    private File inputFile;            // the client-side CSV file that has the readings to transfer
    private String outputFileName ;    // the name of the output file to create on the server to store the readings
    private int maxPatchSize;        // the patch size - no of readings to be sent in the payload of a single Data segment

    private Segment dataSeg   ;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server
    private Segment ackSeg  ;          // the protocol Ack segment for receiving ACK segments from the server

    private int timeout;              // the timeout in milliseconds to use for the protocol with timeout (for Part 3)
    private int maxRetries;           // the maximum number of consecutive retries (retransmissions) to allow before exiting the client (for Part 3)(This is per segment)
    private int currRetry;            // the current number of consecutive retries (retransmissions) following an Ack loss (for Part 3)(This is per segment)

    private int fileTotalReadings;    // number of all readings in the csv file
    private int sentReadings;         // number of readings successfully sent and acknowledged
    private int totalSegments;        // total segments that the client sent to the server

    // Shared Protocol instance so Client and Server access and operate on the same values for the protocol’s attributes (the above attributes).
    public static Protocol instance = new Protocol();

    /**************************************************************************************************************************************
     **************************************************************************************************************************************
     * For this assignment, you have to implement the following methods:
     *    sendMetadata()
     *      readandSend()
     *      receiveAck()
     *      startTimeoutWithRetransmission()
     *    receiveWithAckLoss()
     * Do not change any method signatures, and do not change any other methods or code provided.
     ***************************************************************************************************************************************
     **************************************************************************************************************************************/
    /*
     * This method sends protocol metadata to the server.
     * See coursework specification for full details.
     */
    public void sendMetadata() {
        try {
            // Count lines in the CSV file
            int lineCount = 0;
            try (BufferedReader reader = new BufferedReader(new FileReader(inputFile))) {
                while (reader.readLine() != null) {
                    lineCount++;
                }
            }
            fileTotalReadings = lineCount;

            //  Build metadata payload
            String payload = fileTotalReadings + "," + outputFileName + "," + maxPatchSize;

            // Convert to bytes from UDP
            byte[] data = payload.getBytes();

            //  Create and send UDP packet
            DatagramPacket packet = new DatagramPacket(data, data.length, ipAddress, portNumber);
            System.out.println("CLIENT: Sending metadata to server...");
            socket.send(packet);
            System.out.println("CLIENT: Metadata sent successfully.");

        } catch (IOException e) {
            System.err.println("CLIENT: Error sending metadata: " + e.getMessage());
            if (socket != null && !socket.isClosed()) {
                socket.close();
            }
        }
    }






    /*
     * This method read and send the next data segment (dataSeg) to the server.
     * See coursework specification for full details.
     */
    public void readAndSend() {
        try {
            StringBuilder batch = new StringBuilder();
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(inputFile));
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                batch.append(line).append("\n");
                count++;
                if (count == maxPatchSize) {
                    // create data segment
                    byte[] payload = batch.toString().getBytes();
                    java.net.DatagramPacket packet = new java.net.DatagramPacket(
                            payload,
                            payload.length,
                            ipAddress,
                            portNumber
                    );
                    socket.send(packet);
                    batch.setLength(0);
                    count = 0;
                }
            }
            reader.close();
        } catch (java.io.IOException e) {
            System.out.println("CLIENT: Error reading or sending data");
            socket.close();
            return;
        }
    }






    /*
     * This method receives the current Ack segment (ackSeg) from the server
     * See coursework specification for full details.
     */
    public boolean receiveAck() {
        try {
            byte[] buffer = new byte[MAX_Segment_SIZE];
            java.net.DatagramPacket packet = new java.net.DatagramPacket(buffer, buffer.length);

            // Wait for Ack from server
            socket.receive(packet);

            //  Use ackseg to store the data recieved
            ackSeg = new Segment();

            System.out.println("CLIENT: Received ACK from server...");

            // Make sure the Ack sequence number matches the Dats segment we sent
            if (ackSeg.getSeqNum() != dataSeg.getSeqNum()){
                System.out.println("CLIENT: Received ACK from server but seq number mismatch");
                return false;
            }
            // ACK is correct
            sentReadings++; // increment count of readings acknowledged
            totalSegments++;

            System.out.println("CLIENT: ACK for the Data segment" + dataSeg.getSeqNum() +
                    "confirmed . Total segments sent = " + totalSegments
            );
            // If all readings have been sent end the program
            if (sentReadings >= fileTotalReadings) {
                System.out.println("CLIENT: All readings have been sent");
                System.out.println( "CLIENT: Total segement sent is" + totalSegments);
                System.exit(0);
            }
            return true;


        } catch (java.io.IOException e) {
            System.out.println("CLIENT: Error reading or sending data");
            socket.close();
            return false;
        }

    }

    /*
     * This method starts a timer and does re-transmission of the Data segment
     * See coursework specification for full details.
     */
    public void startTimeoutWithRetransmission()
    {

    }


    /*
     * This method is used by the server to receive the Data segment in Lost Ack mode
     * See coursework specification for full details.
     */
    public void receiveWithAckLoss(DatagramSocket serverSocket, float loss)  {
        System.exit(0);
    }


    /*************************************************************************************************************************************
     **************************************************************************************************************************************
     **************************************************************************************************************************************
     These methods are implemented for you .. Do NOT Change them
     **************************************************************************************************************************************
     **************************************************************************************************************************************
     **************************************************************************************************************************************/
    /*
     * This method initialises ALL the 14 attributes needed to allow the Protocol methods to work properly
     */
    public void initProtocol(String hostName , String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
        instance.ipAddress = InetAddress.getByName(hostName);
        instance.portNumber = Integer.parseInt(portNumber);
        instance.socket = new DatagramSocket();

        instance.inputFile = checkFile(fileName); //check if the CSV file does exist
        instance.outputFileName =  outputFileName;
        instance.maxPatchSize= Integer.parseInt(batchSize);

        instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
        instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

        instance.fileTotalReadings = 0;
        instance.sentReadings=0;
        instance.totalSegments =0;

        instance.timeout = DEFAULT_TIMEOUT;
        instance.maxRetries = DEFAULT_RETRIES;
        instance.currRetry = 0;
    }


    /*
     * check if the csv file does exist before sending it
     */
    private static File checkFile(String fileName)
    {
        File file = new File(fileName);
        if(!file.exists()) {
            System.out.println("CLIENT: File does not exists");
            System.out.println("CLIENT: Exit ..");
            System.exit(0);
        }
        return file;
    }

    /*
     * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
     */
    private static Boolean isLost(float prob)
    {
        double randomValue = Math.random();  //0.0 to 99.9
        return randomValue <= prob;
    }

    /*
     * getter and setter methods    *
     */
    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public int getMaxPatchSize() {
        return maxPatchSize;
    }

    public void setMaxPatchSize(int maxPatchSize) {
        this.maxPatchSize = maxPatchSize;
    }

    public int getFileTotalReadings() {
        return fileTotalReadings;
    }

    public void setFileTotalReadings(int fileTotalReadings) {
        this.fileTotalReadings = fileTotalReadings;
    }

    public void setDataSeg(Segment dataSeg) {
        this.dataSeg = dataSeg;
    }

    public void setAckSeg(Segment ackSeg) {
        this.ackSeg = ackSeg;
    }

    public void setCurrRetry(int currRetry) {
        this.currRetry = currRetry;
    }
