//! A Proc's startup state. The owner releases it after the committed start input.
use crate::{
    error::{Code, Error, Result},
    local::StartupRequest,
    storage::{Capture, ProtocolStore},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Hello,
    Opened,
    Captured,
    Adopted,
    Reconciled,
}

pub(crate) enum StartupAction {
    Ack,
    Capture(Capture),
    ConstructEngine,
}

pub(crate) struct Startup {
    seed: Option<[u8; 16]>,
    step: Step,
}

impl Startup {
    pub fn new() -> Self {
        Self {
            seed: None,
            step: Step::Hello,
        }
    }
    pub fn seed(&self) -> Result<[u8; 16]> {
        self.seed
            .ok_or_else(|| Error::new(Code::MonsterIllegal, "Proc has not received hello"))
    }
    pub fn is_reconciled(&self) -> bool {
        self.step == Step::Reconciled
    }

    pub fn receive(
        &mut self,
        request: &StartupRequest,
        store: &mut ProtocolStore,
    ) -> Result<StartupAction> {
        if let StartupRequest::Hello { seed, .. } = request {
            if self.step != Step::Hello {
                return Err(Error::new(
                    Code::MonsterIllegal,
                    "Proc received hello after startup",
                ));
            }
            crate::storage::validate_generation(seed).map_err(|mut error| {
                error.code = Some(Code::RpcIllegal);
                error
            })?;
            // Repeated hello does not consume deterministic random values.
            self.seed = Some(*seed);
            return Ok(StartupAction::Ack);
        }
        let seed = self.seed()?;
        match (request, self.step) {
            (StartupRequest::Open { .. }, Step::Hello) => {
                store.open(&seed)?;
                self.step = Step::Opened;
                Ok(StartupAction::Ack)
            }
            (StartupRequest::Capture { .. }, Step::Opened) => {
                let capture = store.capture()?;
                self.step = Step::Captured;
                Ok(StartupAction::Capture(capture))
            }
            (StartupRequest::Adopt { .. }, Step::Captured) => {
                store.adopt()?;
                self.step = Step::Adopted;
                Ok(StartupAction::Ack)
            }
            (StartupRequest::Reconcile { .. }, Step::Adopted) => {
                store.reconcile()?;
                self.step = Step::Reconciled;
                Ok(StartupAction::ConstructEngine)
            }
            _ => Err(Error::new(
                Code::MonsterIllegal,
                "Proc startup step is illegal",
            )),
        }
    }
}
