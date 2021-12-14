import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import io.nats.client.support.JsonUtils;

public class jetstraemSender {
    public static void main(String[] args) {
        try(Connection nc= Nats.connect("nats://localhost:4222")){
            JetStreamManagement jsm=nc.jetStreamManagement();
            StreamConfiguration streamConfig = StreamConfiguration.builder()
                    .name("hello")
                    .subjects("com.nats")
                    .storageType(StorageType.Memory)
                    .build();
            StreamInfo streamInfo = jsm.addStream(streamConfig);
            System.out.println(streamInfo);
//            System.out.println(nc.getStatus());
//            JetStream js = nc.jetStream();
//            PublishAck ack = js.publish("com.nats", "ola".getBytes());
//            JsonUtils.printFormatted(ack);
        }catch (Exception e){e.printStackTrace();}
    }
}
