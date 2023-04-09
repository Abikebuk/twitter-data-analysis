import {ETwitterStreamEvent, TwitterApi} from 'twitter-api-v2'
import {MongoClient} from "mongodb";

/**
 * Twitter API keys
 * @type {{access: *, bearer: *, keySecret: *, key: *, accessSecret: *}}
 */
const twitterKeys = {
     key : process.env.TWITTER_KEY,
     keySecret : process.env.TWITTER_KEY_SECRET,
     bearer : process.env.TWITTER_BEARER,
     access :  process.env.TWITTER_ACCESS,
     accessSecret :  process.env.TWITTER_ACCESS_SECRET
}
/**
 * Mongodb host uri
 * @type {MongoClient}
 */
const mongo = new MongoClient(process.env.MONGODB_HOST)

/**
 * Variables initialization
 */
let count = 0
let subCount = 0
let userCount = 0

/**
 * TwitterApi library initialization
 * @type {TwitterApi}
 */
let twitter = new TwitterApi(twitterKeys.bearer)

/**
 * MongoDB database variables
 * Data goes per default in the database "Twitter"
 * Tweets in the colleciton "tweets"
 * Users in the collection "users"
 * Tweets in relation with the one fetched in "tweetsSub" < Those are the tweet the user replied to or RT
 */
const db = mongo.db("twitter")
const tweets = db.collection("tweets")
const users = db.collection("users")
const tweetsSub = db.collection("tweetsSub")

/**
 * I try to avoid duplicate in this section
 */
/**
 * Index creation
 * Unique index on id < which is tweet id on twitter
 */
await tweets.createIndex({
     "id": 1
}, {
     unique : true,
})

/**
 * Index creation
 * Unique index on id < which is tweet id on twitter
 */
await tweetsSub.createIndex({
     "id": 1
}, {
     unique : true,
})

/**
 * Index creation
 * Unique index on id < which is user id on twitter
 */
await users.createIndex({
     "id": 1
}, {
     unique : true,
})

/**
 * Insert the data in the database
 * Data are divided in the three collection (tweets, tweetsSub, users)
 * @param t tweet fetched
 * @returns {Promise<void>}
 */
async function insertData(t) {
     try{
          const ins = tweets.insertOne(t.data)
          console.log(`================ Insert Data #${count++} ================`)
          //console.log(`     Data value : `)
          //console.log(t.data)
          insertUsers(t.includes.users)
          insertTweetsSub(t.includes.tweets)
     }catch {
          console.log(`================ Insert Data #${count+1} ================`)
          console.log(`################ FAILED             ################`)
     }
}

/**
 * Insert an user in the database
 * @param u user fetched
 */
function insertUsers(u){
     u.forEach((_u) => {
          users.updateOne({id: _u.id}, {$set : _u}, {upsert: true}).then((res) => {
               console.log(`xxxxxxxxxxx Insert Sub #${subCount++} xxxxxxxxxxx`)
          }).catch((e) => {
               console.log(`xxxxxxxxxxx FAILED Insert Sub #${subCount+1} xxxxxxxxxxx`)
               console.log(e)
          })
     })
}

/**
 * Insert a tweet in the tweetSub collection
 * @param t tweet fetched
 */
function insertTweetsSub(t){
     t.forEach((_t) => {
          tweetsSub.updateOne({id: _t.id}, {$set : _t}, {upsert: true}).then((res) =>{
               console.log(`xxxxxxxxxxx Insert User #${userCount++} xxxxxxxxxxx`)
          }).catch((e) => {
               console.log(`xxxxxxxxxxx FAILED Insert User #${userCount+1} xxxxxxxxxxx`)
               console.log(e)
          })
     })
}

/**
 * Initialize the stream of the 1% Twitter stream
 * Lot of configuration are used to get the maximum information as possible in the process
 * @type {TweetStream<TweetV2SingleResult>}
 */
const stream = await twitter.v2.sampleStream({
     expansions : [
          "author_id",
          "referenced_tweets.id",
          "referenced_tweets.id.author_id",
          "entities.mentions.username",
          "attachments.poll_ids",
          "attachments.media_keys",
          "in_reply_to_user_id",
          "geo.place_id",
          "edit_history_tweet_ids"
     ],
     "media.fields" : [
          "duration_ms",
          "height",
          "media_key",
          "preview_image_url",
          "type",
          "url",
          "width",
          "public_metrics",
          "alt_text",
          "variants"
     ],
     "place.fields" : [
          "contained_within",
          "country",
          "country_code",
          "full_name",
          "geo",
          "id",
          "name",
          "place_type"
     ],
     "poll.fields" : [
          "duration_minutes",
          "end_datetime",
          "id",
          "options",
          "voting_status"
     ],
     "tweet.fields" : [
          "attachments",
          "author_id",
          "context_annotations",
          "conversation_id",
          "created_at",
          "entities",
          "geo",
          "id",
          "in_reply_to_user_id",
          "lang",
          "public_metrics",
          "possibly_sensitive",
          "referenced_tweets",
          "reply_settings",
          "source",
          "text",
          "withheld"
     ],
     "user.fields" : [
          "created_at",
          "description",
          "entities",
          "id",
          "location",
          "name",
          "pinned_tweet_id",
          "profile_image_url",
          "protected",
          "public_metrics",
          "url",
          "username",
          "verified",
          "verified_type",
          "withheld"
     ]
})

/**
 * Execution of the stream read & processing
 */
stream.on(ETwitterStreamEvent.Data, insertData)

