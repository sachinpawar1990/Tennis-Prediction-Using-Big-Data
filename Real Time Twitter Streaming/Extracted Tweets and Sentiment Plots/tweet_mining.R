library(tm)
library(qdap)

setwd("C:/Users/HP/Desktop/tmTweets")

folderLocation <- "C:/Users/HP/Desktop/tmTweets/"
fileName <- "federer" # Exclude extension '.txt'

###### LOADING INPUT TRANSCRIPT ######

transcript = paste0(folderLocation, paste0(fileName, ".txt"), collapse = ", ")
transcript_txt = readLines(transcript)
sentence = sent_detect(transcript_txt, language = "en")


# Function to extract relevant sentences
extractSentence <- function(corpusIP, textIP = sentence) {
  result = ""
  for(i in 1:length(textIP)){
    for (j in 1:length(corpusIP)) {
      if(grepl(paste0("\\b", tolower(corpusIP[j]), "\\b"), tolower(textIP[i]))){
        result <- c(result, textIP[i])
      }
    }
  }
  result <- result[-1]
  return(result)
}

positive_tweet_corpus_input = paste0(folderLocation, "positive_tweet_corpus.txt", collapse = ", ") 
positive_tweet_corpus = readLines(positive_tweet_corpus_input)

negative_tweet_corpus_input = paste0(folderLocation, "negative_tweet_corpus.txt", collapse = ", ") 
negative_tweet_corpus = readLines(negative_tweet_corpus_input)


###### EXTRACTING RELEVANT SENTENCES ######

positive_tweet_txt <- extractSentence(positive_tweet_corpus, sentence)
negative_tweet_txt <- extractSentence(negative_tweet_corpus, sentence)

###### MAKING COMPARATIVE BAR CHART ######

positiveTweetCount <- length(positive_tweet_txt)
negativeTweetCount <- length(negative_tweet_txt)

tweetSentimentData <- structure(list(positive=  positiveTweetCount, negative = negativeTweetCount), .Names = c("Positive", "Negative"), class = "data.frame", row.names = c(NA, -1L))

tweetPlot <- barplot(as.matrix(tweetSentimentData), main=paste0(fileName, ' Tweet Sentiment'), ylab = "Frequency", beside=TRUE, col=c('#2CA02C', '#D62728'))

# to print to Pdf
dev.print(pdf, paste0(fileName, '_tweetSentimentPlot.pdf'))