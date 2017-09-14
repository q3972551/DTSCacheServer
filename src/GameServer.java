import java.util.List;

import com.aliyun.drc.clusterclient.ClusterClient;
import com.aliyun.drc.clusterclient.ClusterListener;
import com.aliyun.drc.clusterclient.DefaultClusterClient;
import com.aliyun.drc.clusterclient.RegionContext;
import com.aliyun.drc.clusterclient.message.ClusterMessage;


public class GameServer
{
	public static void main(String[] args) throws Exception {
		// logger.info("start");
		// 创建一个context
		
		
		RegionContext context = new RegionContext();
		// 运行SDK的服务器是否使用公网IP连接DTS
		context.setUsePublicIp(true);
		// 用户accessKey secret
		context.setAccessKey(DTSMySQL2RedisProvider.accessKey);
		context.setSecret(DTSMySQL2RedisProvider.accessSecret);
		
		// 创建消费者
		final ClusterClient client = new DefaultClusterClient(context);
		// 创建订阅监听者listener
		ClusterListener listener = new ClusterListener() {
			@Override
			public void noException(Exception e) {
				e.printStackTrace();
			}
			@Override
			public void notify(List<ClusterMessage> messages) throws Exception {
				DTSMySQL2RedisProvider provider = DTSMySQL2RedisProvider.getInstance();
				for (ClusterMessage message : messages) {
					// 同步数据到redis
					provider.replicatorMessage(message);
					// 消费完数据后向DTS汇报ACK，必须调用
					message.ackAsConsumed();
				}
			}
		};
		// 添加监听者
		client.addConcurrentListener(listener);
		// 设置请求的订阅通道ID
		client.askForGUID(DTSMySQL2RedisProvider.Subscription_Instance_ID);
		// 启动后台线程， 注意这里不会阻塞， 主线程不能退出
		client.start();
		
		System.out.println("server start");
		// 保证主线程不能退出
		try {
			Thread.sleep(3600 * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
