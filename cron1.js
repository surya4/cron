
const cron = require('node-cron');
const moment = require('moment');
const _ = require('lodash');
const uuid = require('uuid');
const serverId = uuid.v4();

const redis = require("cache"); // redis connection
const { allNotificationsIds } = require('database'); // db connection
const {delay} = require('./common');

const redisKey = "nodeSrv:hourlyCronJob:";
const runOnceKey = "nodeSrv:runOnceKey:";
const SECONDS_IN_HOUR = 3600;

// run code in only instance to avoid job duplicacy
async function runInOneInstance() {  
let runningFlag;

  try {
    // get key from redis to check if this instance has permission
    runinngServer = await redis.getPromise(runOnceKey)
  } catch (error) {
    console.log('error', error)
    return null;
  }

  // check if there is isActive, if not then set it with setNX so no one can overwrite it
  if (!runinngServer || !runinngServer.isActive) {
    try {
      const setStatus = await redis.setNX(runOnceKey, {isActive: serverId}, SECONDS_IN_HOUR);
      // return true as i set it
      runningFlag = setStatus === 1 ? true:false;
    } catch (error) {
      console.log('error', error)
      runningFlag = false;
    }
  } else {
    // if called again then check it and if serverId and isActive value matches go ahead
    if (runinngServer.isActive === serverId) {
      runningFlag = true;
    } else {
      runningFlag = false;
    }
  }
  // let only one instance use the notification and redis
  return runningFlag;  
}


async function hourlyJob () {
  let start_time, end_time, result, notificationList;
  let current_time = moment().format('YYYY-MM-DD HH:mm:ss');

    try {
    canIRun = await runInOneInstance();
  } catch (error) {
    console.log('error', error)
    return null;
  }

  // if server id matches i have the permission to run head
  if (canIRun) {
    start_time = moment(current_time).add(10, 'm').format('YYYY-MM-DD HH:mm:ss');
    end_time = moment(start_time).add(1, 'h').format('YYYY-MM-DD HH:mm:ss');

    try {
      result = await allNotificationsIds(start_time, end_time);
    } catch (error) {
      console.log('error', error)
    }

    // sort all events in order of asc start time
    notificationList = _.orderBy(_.uniqBy(result, 'notif_id'), ['start_time'],['asc']);

    try {
      await redis.setPromise(redisKey, notificationList, SECONDS_IN_HOUR)
    } catch (error) {
      console.log('error', error)
    }

    await nextJobSchedular(current_time);
  }
};

async function nextJobSchedular(originalStartTime) {
  let allRedisNotifications, upcomingNotifications = [], time_out;
  let current_time = moment().format('YYYY-MM-DD HH:mm:ss');
  const startTime = moment().add(10, 'm').format('YYYY-MM-DD HH:mm:ss');

  try {
    // get all notifications to be sent this hour
    allRedisNotifications = await redis.getPromise(redisKey)
  } catch (error) {
    console.log('error', error)
    return;
  }

  // is there is any data in redis process it
  if (allRedisNotifications && allRedisNotifications.length) {
    allRedisNotifications.forEach((event) => {
      // get all events present less than the upcoming startTime
      if(event.start_time <= startTime) {
        upcomingNotifications.push(event.notif_id);
        // remove the selected one from redis
        allRedisNotifications = allRedisNotifications.slice(1);
      }
    });
  }

  // if there is any data in array send notification
  if (upcomingNotifications && upcomingNotifications.length) {
    try {
      // send notification async
      await sendNotifications(upcomingNotifications);
    } catch (error) {
      console.log('error', error)
    }
  }

// remove after sending notification and add set redis with remaining time of hourly cron
  if (allRedisNotifications && allRedisNotifications.length) {
    try {
      // if suppose instance fired ta 12:40pm then till next hour it has 20 mins else 60 mins
      const remaingCronTime = SECONDS_IN_HOUR - (Date.parse(current_time) - Date.parse(originalStartTime))/1000;
      // except selected ones write all notifications in redis
      await redis.setPromise(redisKey, allRedisNotifications, remaingCronTime)
    } catch (error) {
      console.log('error', error)
    }
  }

  
  if (allRedisNotifications && allRedisNotifications.length) {
    // get first event from upcoming list and subtract 10 mins to set delay
    let next_event = moment(allRedisNotifications[0].start_time).subtract(10, 'm').format('YYYY-MM-DD HH:mm:ss');    
    time_out =  Math.abs(Date.parse(next_event) - moment.utc().valueOf());
  } else {  
    time_out = 600000
  }
  
  // call nextJobSchedular again till end of hour with delay till timeout and call nextJobSchedular
  await delay(time_out, nextJobSchedular, originalStartTime);

};

async function sendNotifications(nodeeventIdsArray) {
  // business logic on nodeeventIdsArray
};

// set cron immideately after server is fired up
exports.immediateCron = hourlyJob;
// set cron every hour e.g. at 12:00pm , 01:00pm etc
exports.job = cron.schedule('0 * * * *', hourlyJob);