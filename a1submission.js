/*
 * This is the final script for MongoDB Assignment-1 submission.
 * It shows recommended steps to connect to a database server called a1, 
 * to set default database and to drop a collection's working copy
 * 
 * It also shows you how to supply command line arguments to 
 * a query.
 *
 * The script assumes that you have imported the tweets data
 * to a collection called tweets in a database called a1.
 * 
 * The script should run using the following command 
 * mongo a1submision.js --eval "args=10000"
 * 
 * All the answers(Q1, Q2, Q3, Q4, Q5, Q6, Q4*, Q6*) 
 * are present in this script.
*/

// save the argument to a local variable, assuming it means 
f_count = args

// The following code initialises a new connection to the MongoDB instance 
// running on localhost on the default port. Using the getDB() method, it sets the global db variable to a1 database.

// make a connection to the database server
conn = new Mongo();

// set the default database
db = conn.getDB("a1");

// duplicate the tweets collection and update the created_at type
// the new collection name is tweets_v2
// aggregation pipe line is used to avoid transferring the entire
// collection to the client side

db.tweets.aggregate(
    [
        {
           $project: {
                id: 1,
            user_id: 1,
                retweet_id: 1,
                replyto_id: 1,
                hash_tags:1,
            user_mentions: 1,
                created_at: {
                    $toDate: "$created_at" 
               }
           }
        },  
        {
           $out: 'tweets_v2',
        },
     ]);
    
    db.users.aggregate(
    [
        {$out: "users_v2"}
    ]);

// Indexing the collection
db.tweets_v2.createIndex({user_id:1})
db.tweets_v2.createIndex({created_at:1})
db.tweets_v2.createIndex({retweet_id:1})
db.tweets_v2.createIndex({ replyto_id:1})
db.tweets_v2.createIndex({text:"text"})

// optionally timing the execution
var start = new Date()

//Question-1
cursor=db.tweets_v2.aggregate([
    // Match stage to filter gen tweets
    {
      $match: {
        replyto_id: { $exists: false},
        retweet_id: { $exists: false},
      }
    },
    {
      $facet: {
        forRetweet:[
          {
            $lookup: {
              from: "tweets_v2",
              localField: "id",
              foreignField: "retweet_id",
              as: "retweetData"
            }
          },
          {
            $unwind: "$retweetData"
          },
          {
            $group: {
              _id: null,
              setArr: {
                $addToSet: "$id"
              }
            }
          }
        ],
        forReplyto:[
          {
            $lookup: {
              from: "tweets_v2",
              localField: "id",
              foreignField: "replyto_id",
              as: "replytoData"
            }
          },
          {
            $unwind: "$replytoData"
          },
          {
            $group: {
              _id: null,
              setArr: {
                $addToSet: "$id"
              } 
            }
          }
        ]
      }
    },
    {
      $unwind: "$forRetweet"
    },
    {
      $unwind: "$forReplyto"
    },
    {
      $project: {
        "Number Of Tweets": {
          $size: {
            $setIntersection: ["$forRetweet.setArr", "$forReplyto.setArr"]
          }
        }
      }
    }
  ])
print("Q1 ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Question-2
cursor=db.tweets_v2.aggregate([
    {
      $match: {
        "replyto_id": {$exists: true}
      }
    },
    {
      $lookup: {
        from: 'tweets_v2',
        localField: 'id',
        foreignField: 'retweet_id',
        as: 'retweetData'
      }
    },
    {
      $addFields: {
        retweet_count: {
          $size: "$retweetData"
        }
      }
    },
    {
      $sort: {
        retweet_count: -1
      }
    },
    {
      $project: {
        retweet_count: 1,
        text: 1
      }
    },
    {
        $limit:1
    }
  ])
print("Q2 ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
//Question-3
cursor=db.tweets_v2.aggregate([
    {
      $match: {
        retweet_id: {$exists: false},
        hash_tags: {$exists: true}
      }
    },
    {
      $addFields: {
        firstHashTag: {
          $first: "$hash_tags"
        }
      }
    },
    {
      $sortByCount: {
        $toLower: "$firstHashTag.text"
      }
    },
    {
        $limit:5
    }
  ]);
print("Q3 ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}
//Question-4 (FIRST IMPLEMENTATION)
cursor=db.getCollection("tweets_v2").aggregate(
    [
        { 
            "$match" : { 
                "hash_tags.text" : args//"Ida"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$user_mentions"
            }
        }, 
        { 
            "$group" : { 
                "_id" : "$user_mentions.id"
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "users_v2", 
                "localField" : "_id", 
                "foreignField" : "id", 
                "as" : "userInfo"
            }
        }, 
        { 
            "$sort" : { 
                "userInfo.followers_count" : -1.0
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$userInfo"
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "id" : "$userInfo.id", 
                "name" : "$userInfo.name", 
                "location" : "$userInfo.location", 
                "followers_count" : "$userInfo.followers_count"
            }
        }, 
        { 
            "$limit" : 5.0
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);

print("Q4==========(first implementation) ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Question-5
cursor  = db.tweets_v2.aggregate([
    {
      $match: {
        replyto_id: {$exists: false},
        retweet_id: {$exists: false},
      }
    },
    {
      $lookup: {
        from: 'users_v2',
        localField: 'user_id',
        foreignField: 'id',
        as: 'userInfo'
      }
    },
    {
      $unwind: "$userInfo"
    },
    {
      $match: {
        "userInfo.location": "",
        "userInfo.description": "",
      }
    },
    {
      $count: "tweet_count"
    }
  ])

print("Q5 ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Question-6 (FIRST IMPLEMENTATION)
cursor=db.getCollection("tweets_v2").aggregate(
    [
        { 
            "$match" : { 
                "replyto_id" : { 
                    "$exists" : false
                }, 
                "retweet_id" : { 
                    "$exists" : false
                }
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "tweets_v2", 
                "let" : { 
                    "id" : "$id", 
                    "ts" : "$created_at"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$and" : [
                                    { 
                                        "$eq" : [
                                            "$$id", 
                                            "$retweet_id"
                                        ]
                                    }, 
                                    { 
                                        "$lt" : [
                                            { 
                                                "$toLong" : { 
                                                    "$toDate" : "$created_at"
                                                }
                                            }, 
                                            { 
                                                "$add" : [
                                                    { 
                                                        "$toLong" : { 
                                                            "$toDate" : "$$ts"
                                                        }
                                                    }, 
                                                    3600000.0
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }, 
                    { 
                        "$count" : "total"
                    }
                ], 
                "as" : "retweetData"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$retweetData"
            }
        }, 
        { 
            "$sort" : { 
                "retweetData.total" : -1.0
            }
        }, 
        { 
            "$project" : { 
                "id" : 1.0, 
                "retweet_count" : "$retweetData.total", 
                "_id" : 0.0
            }
        }, 
        { 
            "$limit" : 1.0
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);
print("Q6==========(first implementation) ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Question-4-SECOND IMPLEMENTATION
cursor= db.getCollection("tweets_v2").aggregate(
    [
        { 
            "$unwind" : { 
                "path" : "$hash_tags"
            }
        }, 
        { 
            "$match" : { 
                "$and" : [
                    { 
                        "hash_tags.text" : args//"Ida"
                    }, 
                    { 
                        "user_mentions" : { 
                            "$exists" : true
                        }
                    }
                ]
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "users_v2", 
                "localField" : "user_mentions.id", 
                "foreignField" : "id", 
                "as" : "embeddedUM"
            }
        }, 
        { 
            "$project" : { 
                "user_mentions.id" : 1.0, 
                "embeddedUM" : 1.0
            }
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "user_mentions.id" : 1.0, 
                "embeddedUM.id" : 1.0, 
                "embeddedUM.name" : 1.0, 
                "embeddedUM.location" : 1.0, 
                "embeddedUM.followers_count" : 1.0, 
                "size_um_arr" : { 
                    "$size" : "$embeddedUM"
                }
            }
        }, 
        { 
            "$match" : { 
                "size_um_arr" : { 
                    "$gt" : 0.0
                }
            }
        }, 
        { 
            "$group" : { 
                "_id" : "$embeddedUM"
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$_id"
            }
        }, 
        { 
            "$project" : { 
                id:"$_id.id",
                name:"$_id.name",
                location:"$_id.location",
                followers_count:"$_id.followers_count"
            }
        }, 
        { 
            "$sort" : { 
                "_id.followers_count" : -1.0
            }
        }, 
        { 
            "$limit" : 5.0
        }, 
        { 
            "$project" : { 
                "_id" : 0.0, 
                "id" : 1.0,
                "name" : 1.0, 
                "location" : 1.0,
                "followers_count" : 1.0
            }
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);

print("Q4==========(second implementation) ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

//Question-6 (SECOND IMPLEMENTATION)
cursor=db.getCollection("tweets_v2").aggregate(
    [
        { 
            "$addFields" : { 
                "timeStampedDate" : { 
                    "$toLong" : { 
                        "$toDate" : "$created_at"
                    }
                }
            }
        }, 
        { 
            "$lookup" : { 
                "from" : "tweets_v2", 
                "let" : { 
                    "id" : "$id", 
                    "ts" : "$created_at"
                }, 
                "pipeline" : [
                    { 
                        "$match" : { 
                            "$expr" : { 
                                "$and" : [
                                    { 
                                        "$eq" : [
                                            "$$id", 
                                            "$retweet_id"
                                        ]
                                    }, 
                                    { 
                                        "$lt" : [
                                            { 
                                                "$toLong" : { 
                                                    "$toDate" : "$created_at"
                                                }
                                            }, 
                                            { 
                                                "$add" : [
                                                    { 
                                                        "$toLong" : { 
                                                            "$toDate" : "$$ts"
                                                        }
                                                    }, 
                                                    3600000.0
                                                ]
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    }, 
                    { 
                        "$count" : "total"
                    }
                ], 
                "as" : "retweetData"
            }
        }, 
        { 
            "$match" : { 
                "replyto_id" : { 
                    "$exists" : false
                }, 
                "retweet_id" : { 
                    "$exists" : false
                }
            }
        }, 
        { 
            "$unwind" : { 
                "path" : "$retweetData"
            }
        }, 
        { 
            "$sort" : { 
                "retweetData.total" : -1.0
            }
        }, 
        { 
            "$project" : { 
                "id" : 1.0, 
                "retweet_count" : "$retweetData.total", 
                "_id" : 0.0
            }
        }, 
        { 
            "$limit" : 1.0
        }
    ], 
    { 
        "allowDiskUse" : false
    }
);
print("Q6==========(second implementation) ====================")
// display the result
while ( cursor.hasNext() ) {
    printjson( cursor.next() );
}

// Dropping the indexes
db.tweets_v2.dropIndex({user_id:1})
db.tweets_v2.dropIndex({created_at:1})
db.tweets_v2.dropIndex({retweet_id:1})
db.tweets_v2.dropIndex({ replyto_id:1})

// drop the newly created collection
db.tweets_v2.drop()
db.users_v2.drop()

