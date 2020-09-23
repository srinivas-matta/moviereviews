## Movie Reviews

### Movie Reviews Components Workflow

1) `MovieReviewsDataGenerator` produce the data for Movie Reviews Application.

2) `CalcAvgVotesApp` reads `title.ratings` data and generates `average number of votes`

3) `CalcTopMovies` reads `title.ratings`, `title.akas`, `average number of votes` and 
   produce the `top movies`

4) `CalcMovieNamesCreditedPeople`  `reads title.principal`, `title.basics`, `top movies` 
   and produce the `related movie names`, `credited peoples` for the top movies.



![MovieReviews_Workflow](MovieReviews_Workflow.png)

