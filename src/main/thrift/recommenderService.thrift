service RecommenderEngine{
    list<i32> getRecommendations(1:i32 userId, 2:i32 numberOfRecommendation, 3:list<i32> currentItemIds)
    bool reLoadDataAndBuildModel()
}
