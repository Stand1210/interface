
@Path("/sorted/check")
@GET
@Produces( { MediaType.APPLICATION_JSON })
@BaseInfo(cate = Category.TIMELINE,
        idx = 1,
        desc = "热门评论进redis情况随机检测",
        rateLimit = RateLimit.USER_IP,
        needAuth = AuthType.OPTION,
        responseType = DataTypes.STRING,
        errorCodes = { "" },
        status = ApiStatus.PUBLIC,
        level = ApiLevel.UNCORE)
public String getHotFlowScore (
@Context AuthResponse authResponse,
//@ParamDesc(isRequired = true, desc = "微博mid") @QueryParam("statusId") String mid,
@ParamDesc(isRequired = false, desc = "热门微博mids_Json") @QueryParam("hotStatuses") String hotStatuses,
@ParamDesc(isRequired = false, range = "int:1~-1", desc = "一级评论id或者二级评论id") @QueryParam("needScoreId") long needScoreId,
@ParamDesc(isRequired = false, range="int:0~1", desc = "是否只查询CounterService中的分数。1为是，0为否") @QueryParam("rankScore") @DefaultValue("0") int rankScore,
@ParamDesc(isRequired = false, range = "enum:0,1,2", desc = "分数类型（0：默认分数、1：新热门流分数、先审后放分数）") @QueryParam("scoreType") @DefaultValue("0") String scoreType,
@ParamDesc(isRequired = false, range = "int:1~1000", desc = "一级评论id或者二级评论的数目") @QueryParam("count") @DefaultValue("300") int count,

@ParamDesc(isRequired = false, range="long:0~-1", desc = "若指定此参数，则只返回ID比since_id大的微博消息（即比since_id发表时间晚的微博消息）。") @QueryParam("since_id") @DefaultValue("0") Long since_id,
@ParamDesc(isRequired = false, range="long:0~-1", desc = "若指定此参数，则返回ID小于或等于max_id的微博消息。") @QueryParam("max_id") @DefaultValue("0") Long max_id,
@ParamDesc(isRequired = false, range = "int:1~100", desc = "每次返回的楼层记录数。") @QueryParam("count") @DefaultValue("15") Integer treeRootcount,
@ParamDesc(isRequired = false, range = "int:1~20", desc = "每次返回的楼层下子评论记录数。") @QueryParam("child_count") @DefaultValue("2") Integer childCount,
@ParamDesc(isRequired = false, range = "int:1~-1", desc = "页码。返回的结果的页码，不限制页数。") @QueryParam("page") @DefaultValue("1") Integer page,
@ParamDesc(isRequired = false, range = "int:0~1", desc = "是否按照时间升序展示。0：降序，1：升序。") @QueryParam("is_asc") @DefaultValue("0") Integer isAsc,
@ParamDesc(isRequired = false, range = "int:0~1", desc = "status中的user信息开关，打开trim时，status中的user字段仅返回user_id，关闭时返回完整user信息。取值：1：关闭user信息，0：打开user信息。默认关闭user信息。") @QueryParam("trim_user") @DefaultValue("0")  int trim_user,
@ParamDesc(isRequired = false, desc = "微博ID。",range="long:1~-1") @QueryParam("id") Long id,
@ParamDesc(isRequired = false, range = "int:0~1", desc = "控制返回结果是否转义。0：不转义，1：转义。") @QueryParam("is_encoded") @DefaultValue("0") Integer isEncoded) throws Exception {

        String midsFile = "/data1/keqi_v4/mids.txt";
        hotStatuses = readToString(midsFile);

        ArrayList<String> midsList = new ArrayList<String>();
        JsonWrapper rtJson = new JsonWrapper(hotStatuses);
        JsonNode statuses = rtJson.getRootNode();
        for  (JsonNode single_status : statuses) {
        JsonNode status = single_status.get("mid");
        midsList.add(status.getValueAsText());
        }
        String mid = null;
        Random random = new Random();
        while(mid == null) {
        int index = random.nextInt(midsList.size());
        mid = midsList.get(index);
        }
//mid下Asc10000条cid
        String[] cidStrAsc = getAscCids(1, (long)0, (long)0, Long.parseLong(mid), 10, page, trim_user, isEncoded, authResponse, childCount);
//一级评论总数
        Map<Long, Integer> statusFloorMap = Maps.newHashMap();
        Future<Boolean> statusFloorFuture = commentHotFlowResourceAggrator.getStatusFloorFuture(Long.parseLong(mid), statusFloorMap);
        AggregatorHelper.getFutureResult(statusFloorFuture, 200);
        long firstFloorNum = statusFloorMap.get(Long.parseLong(mid));
//维护<score, id>  TreeMap
        TreeMap<String, Double> cidsScoreMap = new TreeMap<String, Double>();
        for (int i = 0; i < cidStrAsc.length; i++) {
        double scoreOfEach = getCidScore(Long.parseLong(cidStrAsc[i]), Long.parseLong(cidStrAsc[i]),1,  scoreType, rankScore);
        cidsScoreMap.put(cidStrAsc[i], scoreOfEach);
        }
        List<Map.Entry<String, Double>> list = new ArrayList<Map.Entry<String, Double>>((Collection<? extends Entry<String, Double>>) cidsScoreMap.entrySet());
        Collections.sort(list,new Comparator<Entry<String,Double>>() {
@Override
public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
        return o2.getValue().compareTo(o1.getValue());
        }
        });
//300
        int index = 0;
        ArrayList<String> hotFlowCids_temp_list = new ArrayList<String>();
        String[] hotFlowCids300 = new String[300];
        for (int i = 0; i < 15; i++) {
        String hotFlowJson = getHotFlowCids(authResponse, Long.parseLong(mid), 30, 0, 1, max_id, 0, "", 0);

        rtJson = new JsonWrapper(hotFlowJson);
        JsonNode hotStatusesNode = rtJson.getJsonNode("root_comments");
        for (JsonNode status : hotStatusesNode) {
        JsonNode idNode = status.get("id");
        hotFlowCids_temp_list.add(idNode.getValueAsText());
        }
        JsonNode maxIdNode = rtJson.getJsonNode("max_id");
        max_id = Long.parseLong(maxIdNode.getValueAsText());
        }
        for (int i = 0; i < 300; i++) {
        hotFlowCids300[i] = hotFlowCids_temp_list.get(i);
        }
//第300cid分数
        double score_of_300 = getCidScore(Long.parseLong(hotFlowCids300[299]), Long.parseLong(hotFlowCids300[299]), 1, scoreType, rankScore);

        byGrads(mid, hotFlowCids300, score_of_300, firstFloorNum, cidsScoreMap, list, 300, scoreType, rankScore);
        byGrads(mid, hotFlowCids300, score_of_300, firstFloorNum, cidsScoreMap, list, 250, scoreType, rankScore);
        byGrads(mid, hotFlowCids300, score_of_300, firstFloorNum, cidsScoreMap, list, 200, scoreType, rankScore);
        byGrads(mid, hotFlowCids300, score_of_300, firstFloorNum, cidsScoreMap, list, 100, scoreType, rankScore);


        return "check /data1/keqi_v4/logs/wrongSorted.out";
        }

private void byGrads(String mid, String[] hotFlowCids300, double score_of_300, long firstFloorNum, TreeMap<String, Double> cidsScoreMap, List<Entry<String, Double>> list, int grad, String scoreType, int rankScore) throws Exception {
        int index;
        String[] cidsPre_300 = new String[grad];
        Double[] scorePre_300 = new Double[grad];
        index = 0;
        for(Entry<String,Double> mapping:list){
        cidsPre_300[index] = mapping.getKey();
        scorePre_300[index] = mapping.getValue();
        index++;
        if (index >= grad) {
        break;
        }
        }

        int wrong = 0;
        ArrayList<String> wrongCidsList = new ArrayList<String>();
        ArrayList<Double> wrongCidsScore = new ArrayList<Double>();
        double scoreOfGrad = getCidScore(Long.parseLong(hotFlowCids300[grad - 1]), Long.parseLong(hotFlowCids300[grad - 1]),1,  scoreType, rankScore);
        for (int i = 0; i < cidsPre_300.length; i++) {
        if (isInclude(hotFlowCids300, cidsPre_300[i]) == false && scorePre_300[i] > scoreOfGrad) {
        wrong++;
        wrongCidsList.add(cidsPre_300[i]);
        wrongCidsScore.add(scorePre_300[i]);
        }
        }
        File file = new File("/data1/keqi_v4/logs/wrongSorted" + grad + ".out");
        if (file.exists()) {
        System.out.println("存在文件夹或者文件wrongCids" + grad + ".out");
        } else {
        try {
        file.createNewFile();
        } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        }
        }

        try {
        FileWriter fileWriter = new FileWriter(file, true);
        fileWriter.write("wrongNum:" + String.valueOf(wrong) + " mid:" + mid + " " + cidsScoreMap.size() + "/" + firstFloorNum + "\n");
        fileWriter.close();
        } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        }

        File fileWrongCids = new File("/data1/keqi_v4/logs/wrongCids" + grad + ".out");
        if (fileWrongCids.exists()) {
        System.out.println("存在文件夹或者文件wrongCids" + grad + ".out");
        } else {
        try {
        fileWrongCids.createNewFile();
        } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        }
        }

        try {
        FileWriter fileWriter = new FileWriter(fileWrongCids, true);

        String[] wrongCidsArray = new String[wrongCidsList.size()];
        for (int i = 0; i < wrongCidsList.size(); i++) {
        wrongCidsArray[i] = String.valueOf(wrongCidsList.get(i));
        }
        String[] wrongScoreArray = new String[wrongCidsScore.size()];
        for (int i = 0; i < wrongCidsScore.size(); i++) {
        wrongScoreArray[i] = String.valueOf(wrongCidsScore.get(i));
        }
        String wrongCidsString = printWrongCids(wrongCidsArray, wrongScoreArray);
        fileWriter.write(mid + " " + "scoreOfGrad:" + scoreOfGrad + " " + wrongCidsString + "\n");
        fileWriter.close();
        } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        }
        }

public String printWrongCids(String[] wrongCids, String[] wrongScore) {
        StringBuffer wrongCidString = new StringBuffer();
        for (int i = 0; i < wrongCids.length; i++) {
        wrongCidString.append(wrongCids[i]).append("/").append(wrongScore[i]).append(" ");
        }
        return wrongCidString.toString();
        }

public String readToString(String fileName) {
        String encoding = "ISO-8859-1";
        File file = new File(fileName);
        Long filelength = file.length();
        byte[] filecontent = new byte[filelength.intValue()];
        try {
        FileInputStream in = new FileInputStream(file);
        in.read(filecontent);
        in.close();
        } catch (FileNotFoundException e) {
        e.printStackTrace();
        } catch (IOException e) {
        e.printStackTrace();
        }
        try {
        return new String(filecontent, encoding);
        } catch (UnsupportedEncodingException e) {
        System.err.println("The OS does not support " + encoding);
        e.printStackTrace();
        return null;
        }
        }

public boolean isInclude(String[] cids_300, String cid) {
        for (int i = 0; i < cids_300.length; i++) {
        if (cid.equals(cids_300[i])) {
        return true;
        }
        }
        return false;
        }

public String[] getAscCids(Integer isAsc, Long since_id, Long max_id, Long id, Integer count, Integer page, int trim_user, Integer isEncoded, AuthResponse authResponse, Integer childCount) throws Exception {
        ///timeline/tree/root_child接口
        ArrayList<String> all_cids_list = new ArrayList<String>();

        for (int i = 0 ; i < 1000; i++) {
        CommentTreeContext commentContext = new CommentTreeContext();
        commentContext.setRootId(id);
        commentContext.setSinceId(since_id);
        commentContext.setMaxId(max_id);
        commentContext.setCount(count);
        commentContext.setPage(page);
        commentContext.setAsc(1 == isAsc);
        commentContext.setTrimUser(1 == trim_user);
        commentContext.setEncoded(1 == isEncoded);
        commentContext.setAuthResponse(authResponse);
        commentContext.setChildCount(childCount);
        commentContext.setTrimLevel(1);//评论内容中不带Status
        JsonWrapper rtJson = new JsonWrapper(commentsService.getFirstLevelComments(commentContext));

        JsonNode cidsJson = rtJson.getJsonNode("root_comments");
        if (cidsJson.size() <= 0) {
        break;
        }
        for (JsonNode singleCidNode : cidsJson) {
        JsonNode singleCid = singleCidNode.get("id");
        all_cids_list.add(singleCid.getValueAsText());
        }

        JsonNode sinceIdNode = rtJson.getJsonNode("max_id");
        if (sinceIdNode != null) {
        String sinceIdTemp = sinceIdNode.getValueAsText();
        since_id = Long.parseLong(sinceIdTemp);
        }
        }

        String[] cidsAscArray = new String[all_cids_list.size()];
        for (int i = 0; i < cidsAscArray.length; i++) {
        cidsAscArray[i] = all_cids_list.get(i);
        }
        return cidsAscArray;
        }

public String getHotFlowCids(AuthResponse authResponse, long mid, int count, int childCommentCount, int page, Long max_id, int max_id_type, String ext_param, Integer isEncoded) throws Exception {
        //hotflow接口
        if(!UuidHelper.isValidId(mid)) {
        throw new WeiboApiException(ExcepFactor.E_PARAM_ERROR, new Object[] {"mid is invalid"});
        }

        //默认按照max_id去请求热门序
        HotFlowType hotFlowType = HotFlowType.bottomHotFlow;
        if(max_id_type == HotFlowIdType.timeline.getType()) {
        hotFlowType = HotFlowType.bottomTimeline;
        } else if(page == 1 && max_id == 0) {
        hotFlowType = HotFlowType.firstHotFlow;
        }

        CommentHotFlowContext commentHotFlowContext = new CommentHotFlowContext(hotFlowType.getType(), page, count);
        commentHotFlowContext.setMax_idParam(max_id, max_id_type);
        commentHotFlowContext.setChildCount(childCommentCount);
        commentHotFlowContext.setHotflowInterfaceType(StatusHotCommentUtil.HOTFLOW);
        CommentsUtil.getExpireTimeForTest(commentHotFlowContext);

        return commentsService.getCommentsHotFlow(authResponse, mid, commentHotFlowContext, isEncoded == 1, ext_param);
        }
public double getCidScore(long cid, long needScoreId, int count, String scoreType, int rankScore) throws Exception {   //id->cid
        //hotflow接口
        CommentHotFlowRedisType commentHotFlowRedisType = CommentHotFlowRedisType.defaultType;
        ScoreType type = ScoreType.SCORE;

        List<CommentHotFlowMeta> result = Lists.newArrayList();
        if(needScoreId > 0) {
        // 评论全排序上线后可以查看所有评论的分数，rankScore参数为了方便测试
        Double score = commentHotFlowRedisService.getScore(cid, needScoreId, commentHotFlowRedisType);
        int scoreInCS = commentRankScoreService.getCmtScoreByType(needScoreId, type);
        if (score != null && rankScore == 0) {
        result.add(new CommentHotFlowMeta(needScoreId, score));
        } else if (scoreInCS > 0) {
        score = commentRankScoreService.int2DoubleCmtScore(scoreInCS, needScoreId);
        result.add(new CommentHotFlowMeta(needScoreId, score));
        }
        } else {
        List<CommentHotFlowMeta> allMeta = commentHotFlowRedisService.getAll(cid, count, commentHotFlowRedisType);
        if(!CollectionUtils.isEmpty(allMeta)) {
        result.addAll(allMeta);
        }
        }

        List<JsonBuilder> metasJsonList = Lists.newArrayList();
        for(CommentHotFlowMeta meta : result) {
        metasJsonList.add(meta.toJson());
        }

        JsonBuilder resultJson = new JsonBuilder();
        resultJson.append("id", cid);
        resultJson.appendJsonArr("members", metasJsonList);
        String score_temp = resultJson.flip().toString();

        JsonWrapper rtJson = new JsonWrapper(score_temp);
        JsonNode scoreNode = rtJson.getJsonNode("members");
        String real_score = null;
        for (JsonNode cid_score : scoreNode) {
        JsonNode score = cid_score.get("real_score");
        real_score = score.getValueAsText();
        }
        return Double.parseDouble(real_score);
        }

