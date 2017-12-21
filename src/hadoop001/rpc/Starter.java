package hadoop001.rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Builder;
import org.apache.hadoop.ipc.RPC.Server;

public class Starter {

	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Builder builder = new RPC.Builder(new Configuration());
		builder.setBindAddress("ljserver001").setPort(1000).setProtocol(LoginService.class).setInstance(new LoginServiceImpl());
		Server server = builder.build();
		
		server.start();
	}
}
