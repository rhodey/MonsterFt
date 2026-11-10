use crate::error::{Code, Error, Result};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

pub type TimerId = u64;
const MAX_CLOCK: u64 = i64::MAX as u64;

struct Timer<T> {
    due: u64,
    task: T,
    interval: u64,
}

/// Only runnable continuations belong here. Waiting on another component must
/// register an operation to resume on a later input, outside the current DB1 tx.
pub struct Executor<T> {
    timems: u64,
    queue: VecDeque<T>,
    timers: BTreeMap<TimerId, Timer<T>>,
    next_timer: TimerId,
    running: bool,
}

impl<T> Default for Executor<T> {
    fn default() -> Self {
        Self {
            timems: 0,
            queue: VecDeque::new(),
            timers: BTreeMap::new(),
            next_timer: 0,
            running: false,
        }
    }
}

impl<T> Executor<T> {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn timems(&self) -> u64 {
        self.timems
    }
    pub fn is_running(&self) -> bool {
        self.running
    }
    pub fn enqueue(&mut self, task: T) {
        self.queue.push_back(task);
    }
    pub fn cancel(&mut self, timer: TimerId) {
        self.timers.remove(&timer);
    }

    pub fn clear(&mut self) {
        self.queue.clear();
        self.timers.clear();
    }

    pub fn after(&mut self, delay: u64, task: T, interval: u64) -> Result<TimerId> {
        let due = self
            .timems
            .checked_add(delay)
            .filter(|due| *due <= MAX_CLOCK)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "timer deadline overflow"))?;
        if interval > MAX_CLOCK {
            return Err(Error::new(
                Code::MonsterIllegal,
                "timer interval is illegal",
            ));
        }
        let id = self
            .next_timer
            .checked_add(1)
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "timer identifier overflow"))?;
        self.timers.insert(
            id,
            Timer {
                due,
                task,
                interval,
            },
        );
        self.next_timer = id;
        Ok(id)
    }
}

impl<T: Clone> Executor<T> {
    pub fn run<C>(
        &mut self,
        timems: u64,
        task: T,
        context: &mut C,
        mut dispatch: impl FnMut(&mut C, &mut Self, T) -> Result<()>,
        complete: impl FnOnce(&mut C, &mut Self) -> Result<()>,
    ) -> Result<()> {
        if self.running || timems < self.timems || timems > MAX_CLOCK {
            return Err(Error::new(
                Code::MonsterIllegal,
                "executor input order is illegal",
            ));
        }
        self.running = true;
        self.timems = timems;
        self.enqueue(task);
        let result = self
            .run_work(context, &mut dispatch)
            .and_then(|()| complete(context, self));
        self.running = false;
        self.queue.clear();
        result
    }

    fn drain<C>(
        &mut self,
        context: &mut C,
        dispatch: &mut impl FnMut(&mut C, &mut Self, T) -> Result<()>,
    ) -> Result<()> {
        while let Some(task) = self.queue.pop_front() {
            dispatch(context, self, task)?;
        }
        Ok(())
    }

    fn run_work<C>(
        &mut self,
        context: &mut C,
        dispatch: &mut impl FnMut(&mut C, &mut Self, T) -> Result<()>,
    ) -> Result<()> {
        self.drain(context, dispatch)?;
        let mut fired = BTreeSet::new();
        loop {
            let due = self
                .timers
                .iter()
                .filter(|(id, timer)| timer.due <= self.timems && !fired.contains(*id))
                .min_by_key(|(id, timer)| (timer.due, **id))
                .map(|(id, timer)| (*id, timer.task.clone(), timer.interval));
            let Some((id, task, interval)) = due else {
                break;
            };
            fired.insert(id);
            if interval == 0 {
                self.timers.remove(&id);
            } else {
                let next = self
                    .timems
                    .checked_add(interval)
                    .filter(|due| *due <= MAX_CLOCK)
                    .ok_or_else(|| Error::new(Code::MonsterIllegal, "timer deadline overflow"))?;
                self.timers.get_mut(&id).expect("selected timer exists").due = next;
            }
            self.enqueue(task);
            self.drain(context, dispatch)?;
        }
        Ok(())
    }
}
