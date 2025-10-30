/*
 * Replace the following string of 0s with your student number
 * c4051447
 */
import java.io.*;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.DatagramPacket;


public class Protocol {

    static final String NORMAL_MODE = "nm";         // normal transfer mode: (for Part 1 and 2)
    static final String TIMEOUT_MODE = "wt";        // timeout transfer mode: (for Part 3)
    static final String LOST_MODE = "wl";           // lost Ack transfer mode: (for Part 4)
    static final int DEFAULT_TIMEOUT = 1000;         // default timeout in milliseconds (for Part 3)
    static final int DEFAULT_RETRIES = 4;            // default number of consecutive retries (for Part 3)
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
    private String outputFileName;    // the name of the output file to create on the server to store the readings
    private int maxPatchSize;        // the patch size - no of readings to be sent in the payload of a single Data segment

    private Segment dataSeg;        // the protocol Data segment for sending Data segments (with payload read from the csv file) to the server
    private Segment ackSeg;          // the protocol Ack segment for receiving ACK segments from the server

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
            // Count total lines in the CSV file
            int lineCount = 0;
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(inputFile));
            while (reader.readLine() != null) lineCount++;
            reader.close();
            fileTotalReadings = lineCount;

            // Prepare metadata payload
            String metaPayload = fileTotalReadings + "," + outputFileName + "," + maxPatchSize;

            // Build Metadata segment
            dataSeg = new Segment(0, SegmentType.Meta, metaPayload, metaPayload.length());

            // Serialize Segment
            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
            oos.writeObject(dataSeg);
            oos.flush();

            byte[] metaBytes = baos.toByteArray(); // Convert segment into bytes
            java.net.DatagramPacket packet = new java.net.DatagramPacket(metaBytes, metaBytes.length, ipAddress, portNumber);

            System.out.println("CLIENT: Sending metadata segment to server...");
            socket.send(packet);
            totalSegments++; // Increment the total segment count

            System.out.println("CLIENT: Metadata sent successfully.");
            // Clean up
            oos.close();
            baos.close();
        } catch (java.io.IOException e) {
            // Handling the IOException that may occur during file handling
            System.out.println("CLIENT: Error sending metadata - " + e.getMessage());
            if (socket != null && !socket.isClosed()) socket.close();
        }
    }



    /*
     * This method read and send the next data segment (dataSeg) to the server.
     * See coursework specification for full details.
     */
    public void readAndSend() {
        try {
            java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.FileReader(inputFile));
            StringBuilder batch = new StringBuilder();
            String line;
            int count = 0;
            int seqNum = 1; // Sequence starts at 1 (0 = metadata)

            System.out.println("CLIENT: Beginning data transmission...");
            // Read the file line by line
            while ((line = reader.readLine()) != null) {
                batch.append(line).append("\n");
                count++;
                // If the batch reaches the max patch size or we have reached the end of the file, send the segment
                if (count == maxPatchSize || sentReadings + count == fileTotalReadings) {
                    // Build Data segment
                    dataSeg = new Segment(seqNum, SegmentType.Data, batch.toString(), count);

                    // Serialize
                    java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                    java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
                    oos.writeObject(dataSeg);
                    oos.flush();

                    byte[] segBytes = baos.toByteArray(); // Convert to byte array for transmission
                    java.net.DatagramPacket packet = new java.net.DatagramPacket(segBytes, segBytes.length, ipAddress, portNumber);
                    socket.send(packet);
                    totalSegments++;

                    System.out.println("CLIENT: Sent DATA segment SeqNum=" + seqNum + " (" + count + " readings)");

                    // Wait for ACK
                    if (receiveAck()) {
                        sentReadings += count;
                        seqNum++;
                    }

                    // Reset for next batch
                    batch.setLength(0);
                    count = 0;
                    oos.close();
                    baos.close();
                }
            }

            reader.close();
            System.out.println("CLIENT: All data segments sent successfully.");

        } catch (java.io.IOException e) {
            System.out.println("CLIENT: Error during data send - " + e.getMessage());
            if (socket != null && !socket.isClosed()) socket.close();
        }
    }





    /*
     * This method receives the current Ack segment (ackSeg) from the server
     * See coursework specification for full details.
     */
    public boolean receiveAck() {
        try {
            byte[] buffer = new byte[Protocol.MAX_Segment_SIZE];
            java.net.DatagramPacket packet = new java.net.DatagramPacket(buffer, buffer.length);

            System.out.println("CLIENT: Waiting for ACK...");
            socket.receive(packet);

            // Deserialize ACK
            java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(packet.getData());
            java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
            ackSeg = (Segment) ois.readObject();
            ois.close();
            bais.close();

            // Validate checksum and sequence number
            if (!ackSeg.isValid()) {
                System.out.println("CLIENT: Invalid ACK checksum received.");
                return false;
            }

            if (ackSeg.getSeqNum() != dataSeg.getSeqNum()) {
                System.out.println("CLIENT: Incorrect ACK SeqNum (" + ackSeg.getSeqNum() +
                        "), expected " + dataSeg.getSeqNum());
                return false;
            }

            System.out.println("CLIENT: ACK for SeqNum=" + ackSeg.getSeqNum() + " received and verified.");

            if (sentReadings >= fileTotalReadings) {
                System.out.println("CLIENT: Transfer complete. Total segments sent = " + totalSegments);
                socket.close();
                System.exit(0);
            }

            return true;

        } catch (java.io.IOException e) {
            System.out.println("CLIENT: Error receiving ACK: " + e.getMessage());
            return false;
        } catch (java.lang.ClassNotFoundException e) {
            System.out.println("CLIENT: Error deserializing ACK: " + e.getMessage());
            return false;
        }
    }





    /*
     * This method starts a timer and does re-transmission of the Data segment
     * See coursework specification for full details.
     */

    public void startTimeoutWithRetransmission() {
        try {
            currRetry = 0;  // Reset retry count
            boolean ackReceived = false;

            java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
            java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos);
            oos.writeObject(dataSeg);
            oos.flush();
            byte[] segBytes = baos.toByteArray();

            while (!ackReceived && currRetry < maxRetries) {
                java.net.DatagramPacket packet = new java.net.DatagramPacket(segBytes, segBytes.length);
                socket.send(packet);
                totalSegments++;
                System.out.println("CLIENT: Sent DATA segment SeqNum " + dataSeg.getSeqNum() + " (" + totalSegments + " readings)");

                try {
                    // Wait for ACK with timeout
                    socket.setSoTimeout(timeout);
                    byte[] buffer = new byte[Protocol.MAX_Segment_SIZE];
                    java.net.DatagramPacket ackPacket = new java.net.DatagramPacket(buffer, buffer.length);
                    socket.receive(ackPacket);

                    // Deserialize and process the ACK
                    java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(ackPacket.getData());
                    java.io.ObjectInputStream ois = new java.io.ObjectInputStream(bais);
                    ackSeg = (Segment) ois.readObject();
                    ois.close();
                    bais.close();

                    if (!ackSeg.isValid()) {
                        System.out.println("CLIENT: Invalid ACK received.");
                    } else if (ackSeg.getSeqNum() != dataSeg.getSeqNum()) {
                        System.out.println("CLIENT: Unexpected ACK SeqNum=" + ackSeg.getSeqNum() +
                                " (Expected " + dataSeg.getSeqNum() + ")");
                    } else {
                        System.out.println("CLIENT: ACK received and verified.");
                        ackReceived = true;
                        sentReadings += totalSegments;
                    }

                } catch (java.net.SocketTimeoutException e) {
                    // Timeout occurred, increment retry count and retransmit
                    currRetry++;
                    System.out.println("CLIENT: Timeout, retrying... (Retry count: " + currRetry + ")");
                } catch (java.lang.ClassNotFoundException e) {
                    System.out.println("CLIENT: Error deserializing ACK: " + e.getMessage());
                }

                if (currRetry >= maxRetries) {
                    System.out.println("CLIENT: Max retries reached. Terminating.");
                    break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /*
     * This method is used by the server to receive the Data segment in Lost Ack mode
     * See coursework specification for full details.
     */
    public void receiveWithAckLoss(DatagramSocket serverSocket, float loss) {
        try {
            int expectedSeqNum = 1;  // Start expecting sequence number 1 (after metadata)
            int totalReceivedBytes = 0;
            int totalUsefulBytes = 0;
            Segment lastValidSeg = null;  // To store the last valid Data segment

            long startTime = System.currentTimeMillis();  // Start time for timeout detection

            while (true) {
                byte[] buf = new byte[Protocol.MAX_Segment_SIZE];
                DatagramPacket packet = new DatagramPacket(buf, buf.length);
                serverSocket.receive(packet);

                // Deserialize the received segment
                ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                ObjectInputStream ois = new ObjectInputStream(bais);
                Segment receivedSeg = (Segment) ois.readObject();
                ois.close();
                bais.close();

                // Validate received segment
                if (!receivedSeg.isValid()) {
                    System.out.println("SERVER: Invalid segment received (checksum error).");
                    continue;  // Skip invalid segments
                }

                // If it's metadata, initialize server-side file (handled only once)
                if (receivedSeg.getType() == SegmentType.Meta) {
                    String[] metaData = receivedSeg.getPayLoad().split(",");
                    int totalReadings = Integer.parseInt(metaData[0]);
                    String outputFile = metaData[1];
                    int patchSize = Integer.parseInt(metaData[2]);
                    System.out.println("SERVER: Received metadata — totalReadings=" + totalReadings +
                            ", outputFile=" + outputFile + ", patchSize=" + patchSize);
                    continue;  // Skip metadata processing after the first one
                }

                // Otherwise, assume it's a Data segment
                System.out.println("SERVER: Received DATA segment SeqNum=" + receivedSeg.getSeqNum());

                // Check if the received segment's sequence number is what we expect
                if (receivedSeg.getSeqNum() == expectedSeqNum) {
                    // Valid, new segment: process it
                    System.out.println("SERVER: Processing new Data segment");

                    // Append data to output file
                    BufferedWriter writer = new BufferedWriter(new FileWriter(getOutputFileName(), true));
                    writer.write(receivedSeg.getPayLoad());
                    writer.close();

                    // Update total useful data bytes
                    totalReceivedBytes += receivedSeg.getPayLoad().length();
                    totalUsefulBytes += receivedSeg.getPayLoad().length();

                    // Update the last valid segment
                    lastValidSeg = receivedSeg;

                    // Increment expected sequence number for the next segment
                    expectedSeqNum++;

                    // Simulate ACK loss (may not send ACK for this segment)
                    if (isLost(loss)) {
                        System.out.println("SERVER: Simulating ACK loss for SeqNum=" + receivedSeg.getSeqNum());
                        continue; // Skip sending ACK (simulate loss)
                    }

                    // Send ACK for successfully received segment (without new method)
                    // Serialize the ACK segment
                    ackSeg = new Segment(receivedSeg.getSeqNum(), SegmentType.Ack, "ACK", 0);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(ackSeg);
                    oos.flush();
                    byte[] ackBytes = baos.toByteArray();

                    // Create and send the ACK packet
                    DatagramPacket ackPacket = new DatagramPacket(
                            ackBytes, ackBytes.length, packet.getAddress(), packet.getPort()
                    );
                    serverSocket.send(ackPacket);
                    oos.close();
                    baos.close();

                    System.out.println("SERVER: ACK sent for SeqNum=" + receivedSeg.getSeqNum());

                } else if (receivedSeg.getSeqNum() < expectedSeqNum) {
                    // Duplicate segment: resend the last valid ACK
                    System.out.println("SERVER: Duplicate segment received (SeqNum=" + receivedSeg.getSeqNum() +
                            "). Resending ACK for last valid segment.");

                    // Serialize and send the ACK for the last valid segment
                    ackSeg = new Segment(lastValidSeg.getSeqNum(), SegmentType.Ack, "ACK", 0);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
                    oos.writeObject(ackSeg);
                    oos.flush();
                    byte[] ackBytes = baos.toByteArray();

                    // Create and send the ACK packet
                    DatagramPacket ackPacket = new DatagramPacket(
                            ackBytes, ackBytes.length, packet.getAddress(), packet.getPort()
                    );
                    serverSocket.send(ackPacket);
                    oos.close();
                    baos.close();

                    System.out.println("SERVER: Resending ACK for SeqNum=" + lastValidSeg.getSeqNum());

                } else {
                    // Unexpected sequence number: wait for the correct segment
                    System.out.println("SERVER: Received out-of-order Data segment (SeqNum=" + receivedSeg.getSeqNum() +
                            "). Ignoring it and waiting for SeqNum=" + expectedSeqNum);
                }

                // Timeout: Exit after waiting for 2000ms if no valid segment is received
                if (System.currentTimeMillis() - startTime > 2000) {
                    System.out.println("SERVER: Timeout reached. Exiting.");
                    break;
                }
            }

            // Calculate efficiency
            double efficiency = ((double) totalUsefulBytes / totalReceivedBytes) * 100;
            System.out.println("SERVER: Transfer efficiency: " + efficiency + "%");

        } catch (IOException e) {
            System.out.println("SERVER: I/O Error during data receive - " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.out.println("SERVER: Error deserializing data segment - " + e.getMessage());
        }
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
    public void initProtocol(String hostName, String portNumber, String fileName, String outputFileName, String batchSize) throws UnknownHostException, SocketException {
        instance.ipAddress = InetAddress.getByName(hostName);
        instance.portNumber = Integer.parseInt(portNumber);
        instance.socket = new DatagramSocket();

        instance.inputFile = checkFile(fileName); //check if the CSV file does exist
        instance.outputFileName = outputFileName;
        instance.maxPatchSize = Integer.parseInt(batchSize);

        instance.dataSeg = new Segment(); //initialise the data segment for sending readings to the server
        instance.ackSeg = new Segment();  //initialise the ack segment for receiving Acks from the server

        instance.fileTotalReadings = 0;
        instance.sentReadings = 0;
        instance.totalSegments = 0;

        instance.timeout = DEFAULT_TIMEOUT;
        instance.maxRetries = DEFAULT_RETRIES;
        instance.currRetry = 0;
    }


    /*
     * check if the csv file does exist before sending it
     */
    private static File checkFile(String fileName) {
        File file = new File(fileName);
        if (!file.exists()) {
            System.out.println("CLIENT: File does not exists");
            System.out.println("CLIENT: Exit ..");
            System.exit(0);
        }
        return file;
    }

    /*
     * returns true with the given probability to simulate network errors (Ack loss)(for Part 4)
     */
    private static boolean isLost(float prob) {
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
}
