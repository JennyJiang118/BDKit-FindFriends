# BDKit-friends

## 设计思路
* 设计两个mapreduce的job

### 第一个job：mutualFriends用来判断两个人是否是双向好友，目的是排除单向好友的干扰
* map: 将一条输入拆解成user和friends，输出"key=friend, value=user",即"其中一个人的好友，这个人"
> context.write(new Text(friend), new Text(user));//exchange key & value
* reduce: 对所有key相同的进行拼接,输出"key=friend, value=users",即"一个好友，有此好友的所有人"
> StringBuffer sb = new StringBuffer();
			for(Text friend : values){
				sb.append(friend.toString()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			context.write(key, new Text(sb.toString()));

### 第二个job: findFriends用来找共同好友
* map: 对“有此好友的所有人进行两两配对”，输出"user1-user2, commonFriend",即"两名用户，他们的一个共同好友"
> for(int i=0;i<users.length-1;i++){
				for(int j=i+1;j<users.length;j++){
					context.write(new Text("(["+users[i]+","+users[j]+"],"), new Text(friend));
				}
			}

* reduce: 对所有key相同的进行拼接，即统计任意两名用户的所有共同好友
> protected void reduce(Text key, Iterable<Text> values,Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			Set<String> set = new HashSet<String>();
			for(Text friend : values){
				if(!set.contains(friend.toString()))//排除多个共同好友的重复计数
					set.add(friend.toString());
			}
			for(String friend : set){
				sb.append(friend.toString()).append(",");
			}
			sb.deleteCharAt(sb.length()-1);
			context.write(key, new Text("["+sb.toString()+"])"));
		}

## 细节问题
### findFriendMapper中，在两两配对前应首先对每一对数据内的用户id进行排序，避免重复计数用户对。例如，考虑客户100和客户200，若输入有：
* 300, 200 100
* 400, 100 200
* 那么此时会出现“200，100”和“100，200”两种用户对，但这两种表示方法本质上都是用户100和用户200的用户对。为避免这种用户对的重复计数，应首先对用户进行排序。
> String line = value.toString();
			String[] friendUsers = line.split("\t");
			String friend = friendUsers[0];
			String[] users = friendUsers[1].split(",");
			Arrays.sort(users); //sort 防止重复

### findFriendReducer中，在合并拼接时，应首先判断set中是否已存在该共同好友。
> for(Text friend : values){
				if(!set.contains(friend.toString()))//排除多个共同好友的重复计数
					set.add(friend.toString());
			}

