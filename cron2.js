
const cron = require('node-cron');
const prometheus = require('@noon/prometheus');
const moment = require('moment');
const _ = require('lodash');
const uuid = require('uuid');
const serverId = uuid.v4();

const redis = require("cache"); //redis connection
const {  allNotificationsIds } = require('database'); //db connection
const {delay, secondsLeftToCompleteHour, minutesLeftToCompleteHour} = require('./common');

const redisKey = "nodeSrv:hourlyCronJob:";
const runOnceKey = "nodeSrv:runOnceKey:";
const SECONDS_IN_HOUR = 3600, MINUTES_IN_HOUR = 60, MILLISECONDS = 1000;

// run code in only instance to avoid job duplicacy
async function runInOneInstance() {  
let runningFlag;

  try {
    // get key from redis to check if this instance has permission
    runinngServer = await redis.getPromise(runOnceKey)
  } catch (error) {
    console.log('error ', error)
    return null;
  }

  // check if there is isActive, if not then set it with setNX so no one can overwrite it
  if (!runinngServer || !runinngServer.isActive) {
    try {
      // set redis expiry till end of hour e.g. if server fired at 12:40 then 20 mins expiry from 12:40pm to 1:00pm and then subsequent hours
      const  expiry = secondsLeftToCompleteHour() < SECONDS_IN_HOUR ? secondsLeftToCompleteHour() : SECONDS_IN_HOUR;
      const setStatus = await redis.setNX(runOnceKey, {isActive: serverId}, expiry);
      runningFlag = setStatus === 1 ? true:false;
    } catch (error) {
      console.log('error ', error)
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
  let start_time, end_time, result;
  let current_time = moment().format('YYYY-MM-DD HH:mm:ss');

    try {
    canIRun = await runInOneInstance();

  } catch (error) {
    console.log('error ', error)
    return null;
  }

  if (canIRun) {
    // time duration for redis lock e.g. if instance fired at 12:40pm then get all event from 12:40pm to 1:00pm and then subsequent hours
    const  end_minutes = minutesLeftToCompleteHour() < MINUTES_IN_HOUR ? minutesLeftToCompleteHour() : MINUTES_IN_HOUR;
    start_time = moment(current_time).add(10, 'm').format('YYYY-MM-DD HH:mm:ss');
    end_time = moment(start_time).add(end_minutes-1, 'm').format('YYYY-MM-DD HH:mm:ss');

    try {
      // get all event in the given period
      result = await allNotificationsIds(start_time, end_time);
    } catch (error) {
      console.log('error ', error)
    }

    // formulate and get the event for the given peroiod for each minute basis and set in redis
    const notification_slots = await formulateAndAddevents(result);
    if (notification_slots && notification_slots.length) {
      // run a loop on the above given time sets or every 10 mins
      await nextJobSchedular(notification_slots, index = 0);
    }
  }
};

// formulate them and set in redis, run a lot on it
async function formulateAndAddevents(events) {
  let available_slots = [];
  // subtract 10 mins from event firing time and set in redis
  events.forEach(el => {
    el.start_time = moment(el.start_time).subtract(10, 'm').format('YYYY-MM-DD HH:mm:ss');    
  })
  // sort all events in order of asc start time
  let notificationList = _.orderBy(_.uniqBy(events, 'notification_logger_id'), ['start_time'],['asc']);
  
  // set events ids for each minute
  for (let i = 0; i <= 59; i++) {
    let events = [];
    _.map(notificationList, (el) => {
      if (moment(el.start_time).minutes() == i) {
        events.push(el.notification_logger_id)
      }
    });

    // if there is any event at nay given time set in redis, so we call only when events is present
    // e.g. if there is event running and 3rd minute and next present in 5th minute then we will skip 
    // calling at 4th minute with delay
    if (events && events.length) {
      available_slots.push(i);
      redis.setAdd(`${redisKey}:${i}`, events)
    }
  }

  // e.g. := 3 : [1234123, 4567123], 23 : [6234123, 7557123], 35 : [4234123, 8537123], 53 : [2234123, 9527123],
  return available_slots;
}

async function nextJobSchedular(notification_slots, index) {
  let upcomingevents = [], time_out = 10*60*MILLISECONDS;

  // check if event exists in redis at given time
  if (notification_slots[index]) {
    // if current minute  at given time and key in slot is same then send notification
    if (notification_slots[index] == moment().minutes()) {
      try {
        // all event in given minute e.g. [1234123, 4567123] at 3th minute
        upcomingevents = await redis.getSmembers(`${redisKey}:${notification_slots[index]}`);
      } catch (error) {
        console.log('error ', error)
        return;
      }

      // send notification if there is any event at given time
      if (upcomingevents && upcomingevents.length) {
        try {
          await sendNotifications(upcomingevents);
        } catch (error) {
          console.log('error ', error)
        }
      }

      // delay till next time slot or 10 minutes
      time_out = (notification_slots[index + 1]) ? (notification_slots[index + 1] - notification_slots[index])*60*MILLISECONDS : 10*60*MILLISECONDS;
      index++;
    } else if (notification_slots[index] > moment().minutes())  {
      // if current minute time is less than key in slot then delay till slot arrives
      time_out = (notification_slots[index] - moment().minutes())*60*MILLISECONDS
    }
  }
  // call nextJobSchedular again till end of hour with delay till timeout and call nextJobSchedular
  await delay(time_out, nextJobSchedular, notification_slots, index);
};

async function sendNotifications(notificationeventIdsArray) {
    // business logic on nodeeventIdsArray
};

// set cron immideately after server is fired up
exports.immediateCron = hourlyJob;
// set cron every hour e.g. at 12:00pm , 01:00pm etc
exports.job = cron.schedule('0 * * * *', hourlyJob);