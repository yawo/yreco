service RecommenderEngine{
    set<i32> getRecommendations(1:i32 userId, 2:i32 numberOfRecommendation, 3:set<i32> currentItemIds)
}