service RecommenderEngine{
    list<string> getRecommendations(1:i32 userId, 2:i32 numberOfRecommendation, 3:list<string> currentItemIds)
    list<string> getSimilarProducts(1:string productId)
    bool reLoadDataAndBuildModel(1:i32 nMaxSimsByProd,2:double cooccurenceThresholdSupport)

}
