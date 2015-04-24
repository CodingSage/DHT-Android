package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    private String port;
    private String id;
    private String idhash;
    private final int SERVER_PORT = 10000;
    private List<String> ids;
    private String predecessor;
    private String successor;
    private String dataMsg;
    public ReentrantLock countLock = new ReentrantLock();
    private int dataCount;
    private int totalCount;

    public final String MESSAGE_JOIN_REQUEST = "REQUEST";
    public final String MESSAGE_JOIN_CONFIRMATION = "CONFIRMATION";
    public final String MESSAGE_DATA_DELETE = "DATAD";
    public final String MESSAGE_DATA_INSERT = "DATAI";
    public final String MESSAGE_DATA_REQUEST = "DATAQ";
    public final String MESSAGE_DATA_LIST = "DATAL";

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        Log.i("DATA", "Delete on " + id + ":" +selection);
        if(!checkHash(selection)){
            Message msg = new Message(MESSAGE_DATA_DELETE, selection);
            int sport = Integer.parseInt(successor) * 2;
            sendMessage(msg, sport);
            //new ClientTask(port).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg);
            return 0;
        }
        List<String> files = Arrays.asList(getContext().fileList());
        if(selection.equals("\"*\"") || selection.equals("\"@\"")){
            for(String key : files)
                delete(key);
            if(selection.equals("\"@\"")){
                Message msg = new Message(MESSAGE_DATA_DELETE, selection);
                int sport = Integer.parseInt(successor) * 2;
                sendMessage(msg, sport);
                //new ClientTask(sport).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg);
            }
        } else {
            if (files.contains(selection)) {
                delete(selection);
            }
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        Log.i("DATA", "Insert on " + id + "=>" + key + ":" + value);
        if(!checkHash(key)){
            Message msg = new Message(MESSAGE_DATA_INSERT, key + ">>" + value);
            int sport = Integer.parseInt(successor) * 2;
            sendMessage(msg, sport);
            Log.i("DATA", "Forwarded message to " + sport/2);
            return uri;
        }
        save(key, value);
        return uri;
    }

    @Override
    public boolean onCreate() {
        dataMsg = "";
        dataCount = 0;
        totalCount = 0;
        predecessor = "";
        successor = "";
        ids = new ArrayList<>();
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        id = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        port = String.valueOf((Integer.parseInt(id) * 2));

        try {
            idhash = genHash(id);
            ids.add(id);
        } catch (NoSuchAlgorithmException e) {
            Log.e("CONTENT_PROVIDER", e.getMessage());
        }

        ServerSocket serverSocket = null;
        try {
            serverSocket = new ServerSocket(SERVER_PORT);
        } catch (IOException e) {
            Log.e("CONTENT_PROVIDER", "Error creating server socket");
        }
        new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

        if(!id.equals("5554")) {
            //send data to 5554 about joining
            Message msg = new Message(MESSAGE_JOIN_REQUEST, id);
            Log.i("DEBUG", "Requesting to join chord from " + id);
            new ClientTask(11108).executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
            Log.i("DEBUG", "Sent request from " + id);
        }
        Log.i("DEBUG", "Starting up node=> " + id);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        Log.i("DATA", "Query on " + id + ":" +selection);
        List<String> values = new ArrayList<>();
        List<String> keys = new ArrayList<>();
        if(selection.equals("\"*\"") || selection.equals("\"@\"")){
            String[] lkeys = getContext().fileList();
            for(String key : lkeys){
                values.add(fetch(key));
                keys.add(key);
            }
            if(selection.equals("\"*\"")){
                Message msg = new Message(MESSAGE_DATA_REQUEST, "\"@\"");
                msg.setSenderport(Integer.parseInt(port));
                //countLock.lock();
                dataCount = 0;
                totalCount = ids.size()-1;
                for(String lid : ids){
                    if(lid.equals(this.id))
                        continue;
                    int sport = Integer.parseInt(lid)*2;
                    sendMessage(msg, sport);
                    //new ClientTask(port).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg);
                    Log.i("DATA", "Query forwarded to " + lid);
                }
                //wait for all data
                while(dataCount < totalCount) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if(!dataMsg.isEmpty()) {
                    String[] pairs = dataMsg.split("\\|");
                    Log.i("DATA", "Number of data received " + pairs.length);
                    for (String p : pairs) {
                        String[] kv = p.split(">>");
                        keys.add(kv[0]);
                        values.add(kv[1]);
                    }
                    dataMsg = "";
                }
                //countLock.unlock();
            }
        } else {
            if(!checkHash(selection)){
                //countLock.lock();
                dataCount = 0;
                totalCount = 1;
                Message msg = new Message(MESSAGE_DATA_REQUEST, selection);
                msg.setSenderport(Integer.parseInt(port));
                int sport = Integer.parseInt(successor)*2;
                sendMessage(msg, sport);
                //new ClientTask(sport).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg);
                Log.i("DATA", "Forwarded request to " + successor);
                //wait for data
                while(dataCount < totalCount) {
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                String[] kv = dataMsg.split(">>");
                keys.add(kv[0]);
                values.add(kv[1]);
                dataMsg = "";
            }else{
                keys.add(selection);
                values.add(fetch(selection));
            }
            //countLock.unlock();
        }
        String[] cols = {"key", "value"};
        MatrixCursor cursor = new MatrixCursor(cols);
        for(int i = 0; i < keys.size(); i++){
            List<String> row = new ArrayList<String>();
            row.add(keys.get(i));
            row.add(values.get(i));
            cursor.addRow(row);
            Log.i("DATA", "Returning query value for " + selection + " => " + keys.get(i) + ":" + values.get(i));
        }
        Log.i("DATA", "Total data returned " + keys.size());
        Log.v("query", selection);
        return cursor;
    }

    private boolean checkHash(String key) {
        String hash = "";
        String prehash = "";
        if(predecessor.equals("") || successor.equals(""))
            return true;
        try {
            hash = genHash(key);
            prehash = genHash(predecessor);
        } catch (NoSuchAlgorithmException e) {
            Log.e("SERVER", e.getMessage());
        }
        if(hash.compareTo(idhash) <= 0 && hash.compareTo(prehash) > 0)
            return true;
        if(id.equals(ids.get(0)) && hash.compareTo(prehash) > 0)
            return true;
        if(id.equals(ids.get(0)) && hash.compareTo(idhash) <= 0)
            return true;
        return false;
    }

    private String fetch(String key) {
        String val = "";
        try {
            InputStream stream = getContext().openFileInput(key);
            BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
            val = reader.readLine();
            Log.i("DATA", "data retrieved " + key + ">>" + val);
        } catch (IOException e) {
            Log.v("EXCEPTION", e.getMessage());
        }
        return val;
    }

    private void save(String key, String value) {
        try {
            Log.i("DATA", "Saved on " + id + " " + key + ":" + value);
            OutputStream stream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
            stream.write(value.getBytes());
            stream.close();
        } catch (IOException e) {
            Log.v("EXCEPTION", e.getMessage());
        }
    }

    private void sendMessage(Message param, int sport) {
        try {
            Message msgToSend = param;
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), sport);
            OutputStream stream = socket.getOutputStream();
            ObjectOutputStream os = new ObjectOutputStream(stream);
            os.writeObject(msgToSend);
            socket.close();
        } catch (UnknownHostException e) {
            Log.e("CLIENT", "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e("CLIENT", "ClientTask socket IOException");
        }
    }

    private void delete(String key){
        Log.i("DATA", "Deleted on " + id + ": " + key);
        getContext().deleteFile(key);
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, Message, Void> {

        @Override
        protected Void doInBackground(ServerSocket... params) {
            while(true) {
                ServerSocket server = params[0];
                try {
                    InputStream stream = server.accept().getInputStream();
                    ObjectInputStream os = new ObjectInputStream(stream);
                    Message msg = (Message) os.readObject();
                    Log.i("DEBUG", "message received " + msg.getType() + " " + msg.getMessage() + " from " + msg.getSenderport());
                    if (msg.getType().startsWith("DATA"))
                        dataOperation(msg);
                    else
                        nodeOperation(msg);
                    os.close();
                } catch (IOException e) {
                    Log.e("SERVER", e.getMessage());
                } catch (Exception e) {
                    Log.e("SERVER", e.getMessage());
                }
            }
        }

        private void nodeOperation(Message message) {
            if(message.getType().equals(MESSAGE_JOIN_REQUEST)){
                Log.i("DEBUG", "calculating join sequence for new node");
                String newid = message.getMessage();
                try {
                    ids.add(newid);
                    Map<String, String> hmap = new TreeMap<>();
                    for(String lid : ids)
                        hmap.put(genHash(lid), lid);
                    ids = new ArrayList<>();
                    for(String key : hmap.keySet())
                        ids.add(hmap.get(key));

                    setNeighbours();
                    sendConfirmation();
                } catch (NoSuchAlgorithmException e) {
                    Log.e("SERVER", e.getMessage());
                }
            } else {
                Log.i("DEBUG", "received new sequence :" + message.getMessage());
                String[] idarray = message.getMessage().split("\\|");
                ids = Arrays.asList(idarray);
                Log.i("DEBUG", "id list updated (size:" + ids.size() + ") " + ids + " from (size: " + idarray.length + ") ");
                setNeighbours();
            }
        }

        private void setNeighbours(){
            Log.i("DEBUG", "setting new neighbours from : " + ids);
            for(int i = 0; i < ids.size(); i++){
                if(ids.get(i).equals(id)){
                    if(i == 0)
                        predecessor = ids.get(ids.size()-1);
                    else
                        predecessor = ids.get(i-1);
                    successor = ids.get((i+1)%ids.size());
                    Log.i("DEBUG", id + "=> Predecessor: " + predecessor + " Successor: " + successor);
                    break;
                }
            }
        }

        private void sendConfirmation() {
            Log.i("DEBUG", "Sending confirmations");
            String confirm = "";
            for(int i = 0; i < ids.size(); i++){
                confirm += ids.get(i);
                if(i != ids.size() - 1)
                    confirm += "|";
            }
            for(String lid : ids){
                if(lid.equals(id))
                    continue;
                int port = Integer.parseInt(lid) * 2;
                Message msg = new Message(MESSAGE_JOIN_CONFIRMATION, confirm);
                Log.i("DEBUG", "Sending confirmation ring " + confirm + " to " + port);
                new ClientTask(port).executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg);
            }
        }

        private void dataOperation(Message message) {
            Log.i("DATA", "data operation " + message.getType() + ":" + message.getMessage() + " from " + message.getSenderport());
            if(!message.getType().endsWith("L") && !message.getMessage().equals("\"@\"")){
                String hashcheck = message.getMessage();
                if(message.getType().equals(MESSAGE_DATA_INSERT))
                    hashcheck = hashcheck.split(">>")[0];
                if(!checkHash(hashcheck)) {
                    Log.i("DATA", "Forwarding message to " + successor);
                    int sport = Integer.parseInt(successor) * 2;
                    sendMessage(message, sport);
                    //new ClientTask(sport).executeOnExecutor(THREAD_POOL_EXECUTOR, message);
                    return;
                }
            }
            if(message.getType().equals(MESSAGE_DATA_INSERT)){
                String[] vals = message.getMessage().split(">>");
                save(vals[0], vals[1]);
            } else if (message.getType().equals(MESSAGE_DATA_REQUEST)){
                String msg = "";
                if(message.getMessage().equals("\"@\"")){
                    String[] lkeys = getContext().fileList();
                    for(int i = 0; i < lkeys.length; i++) {
                        msg += lkeys[i] + ">>" + fetch(lkeys[i]);
                        if(i != lkeys.length-1)
                            msg += "|";
                    }
                } else {
                    msg = message.getMessage() + ">>" + fetch(message.getMessage());
                }
                Message dataMsg = new Message(MESSAGE_DATA_LIST, msg);
                dataMsg.setSenderport(Integer.parseInt(port));
                sendMessage(dataMsg, message.getSenderport());
                //new ClientTask(message.getSenderport()).executeOnExecutor(THREAD_POOL_EXECUTOR, dataMsg);
                Log.i("DATA", "Sending data from " + id + " to " + message.getSenderport() + " " + dataMsg.getMessage());
            } else if (message.getType().equals(MESSAGE_DATA_DELETE)){
                delete(message.getMessage());
                Log.i("DATA", "Deleting data from " + id + ":" + message.getMessage());
            } else if (message.getType().equals(MESSAGE_DATA_LIST)){
                if(!dataMsg.equals("") && !message.getMessage().isEmpty())
                    dataMsg += "|";
                dataMsg += message.getMessage();
                dataCount++;
                Log.i("DATA", "Received data at " + id + " from " + message.getSenderport() + ": " + message.getMessage() + " => " + dataCount + "/" + totalCount);
            } else {
                Log.i("DATA", "Executing last part of data operations");
                String[] keys = getContext().fileList();
                String msg = "";
                for(int i = 0; i < keys.length; i++){
                    String val = fetch(keys[i]);
                    msg += keys[i] + ">>" + val;
                    if(i != keys.length-1)
                        msg += "|";
                }
                Message dataMsg = new Message(MESSAGE_DATA_LIST, msg);
                dataMsg.setSenderport(Integer.parseInt(port));
                Log.i("DATA", "Sending data from " + id + " to " + message.getSenderport() + " " + message.getMessage());
                sendMessage(dataMsg, message.getSenderport());
                //new ClientTask(message.getSenderport()).executeOnExecutor(THREAD_POOL_EXECUTOR, dataMsg);
            }
        }
    }

    private class ClientTask extends AsyncTask<Message, Void, Void> {

        private int port;

        public ClientTask(int port){
            this.port = port;
        }

        @Override
        protected Void doInBackground(Message... params) {
            sendMessage(params[0]);
            return null;
        }

        private void sendMessage(Message param) {
            try {
                Message msgToSend = param;
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
                OutputStream stream = socket.getOutputStream();
                ObjectOutputStream os = new ObjectOutputStream(stream);
                os.writeObject(msgToSend);
                socket.close();
            } catch (UnknownHostException e) {
                Log.e("CLIENT", "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e("CLIENT", "ClientTask socket IOException");
            }
        }
    }

}
