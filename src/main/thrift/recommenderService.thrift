service RecommenderEngine{
    list<i32> getRecommendations(1:i32 userId, 2:i32 numberOfRecommendation, 3:list<i32> currentItemIds)
    list<i32> getSimilarProducts(1:i32 productId)
    bool reLoadDataAndBuildModel()

}
