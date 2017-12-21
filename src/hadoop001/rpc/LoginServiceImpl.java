package hadoop001.rpc;

public class LoginServiceImpl implements LoginService{

	@Override
	public String login(String username, String password) {
		return username+"login success";
	}

}
