package org.exist.messaging.receive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 *
 * @author wessels
 */
public class ReceiversManager {

    //private static List<Receiver> receivers = new ArrayList<Receiver>();
    private Map<String, Receiver> receivers = new HashMap<String, Receiver>();
    
    private static ReceiversManager instance;

    private ReceiversManager() {
        // Nop
    }

    public static ReceiversManager getInstance() {

        if (null == instance) {
            instance = new ReceiversManager();
        }

        return instance;
    }

    public String add(Receiver receiver) {
        
        if(receiver==null){
            throw new IllegalArgumentException("Receiver should not be null");
        }
        
        String id = UUID.randomUUID().toString();
        receivers.put(id, receiver);
        return id;
    }

    public void remove(String id) {
        receivers.remove(id);
    }

    public Receiver get(String id) {
        return receivers.get(id);
    }
    
    public Set<String> getIds(){
        return receivers.keySet();
    }
    
    public String list(){
        
        for(String key : receivers.keySet()){
            Receiver receiver = receivers.get(key);
            
        }
        return null;
    }

}
