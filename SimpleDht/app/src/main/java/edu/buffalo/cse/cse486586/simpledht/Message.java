package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;

/**
 * Created by vinayak on 4/3/15.
 */
public class Message implements Serializable {

    private String type;
    private String message;
    private int senderport;

    public Message(){}

    public Message(String type, String msg){
        this.type = type;
        this.message = msg;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSenderport() {
        return senderport;
    }

    public void setSenderport(int senderport) {
        this.senderport = senderport;
    }
}
