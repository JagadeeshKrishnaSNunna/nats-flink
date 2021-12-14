import io.nats.client.*;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

class nats {
    public static void main(String[] args) throws IOException, InterruptedException {
        Dispatcher dispacher= Nats.connect().createDispatcher(new MessageHandler() {
                        @Override
                        public void onMessage(Message message) throws InterruptedException {
                            System.out.println(message.getConnection().getConnectedUrl());
                            Map<String,String> msg=new HashMap<String,String>();
                            msg.put("id",message.getSID());
                            msg.put("message",new String(message.getData(),StandardCharsets.UTF_8));
                            msg.put("subject",message.getSubject());
                            msg.put("replyTo", message.getReplyTo());
                            msg.put("subscription", message.getSubject());
                            msg.put("connection", message.getConnection().getConnectedUrl());
                            String mssg=msg.toString();
//                            appendFile(mssg);
                          appendFile(msg.get("message"));

                       }
                    });
                    dispacher.subscribe("com.nats");
    }
    static void appendFile(String msg){
        try{
            BufferedWriter out = new BufferedWriter(new FileWriter("/home/jadhu/Desktop/inp.txt"));
            out.write(msg+"\n");
            out.close();
//            flinkReadTextFile.flink();
        }catch (Exception e){}
    }
}