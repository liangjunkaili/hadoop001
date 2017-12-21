package hadoop001.rpc;

public interface LoginService {

	public static final long versionID = 1L;
	public String login(String username,String password);
}
