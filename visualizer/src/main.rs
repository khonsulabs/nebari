mod tree_canvas;

use std::sync::Arc;

use gooey::{
    core::{styles::Autofocus, Context, StyledWidget, WindowBuilder},
    widgets::{
        button::Button,
        component::{Behavior, Component, Content, EventMapper},
        input::Input,
        layout::{Dimension, Layout, WidgetLayout},
    },
    App,
};
use gooey_canvas::{AppExt, Canvas};
use nebari::{
    io::{
        memory::{MemoryFile, MemoryFileManager},
        FileOp, ManagedFile, OpenableFile,
    },
    tree::{ByIdStats, State, TreeFile, UnversionedByIdIndex, UnversionedTreeRoot},
    Buffer,
};
use parking_lot::Mutex;

use crate::tree_canvas::{Tree, TreeCanvas};

fn app() -> App {
    let file = TreeFile::write(
        "path",
        State::default(),
        &nebari::Context {
            file_manager: MemoryFileManager::default(),
            vault: None,
            cache: None,
        },
        None,
    )
    .unwrap();
    let tree = Arc::default();
    App::from(
        WindowBuilder::new(|storage| {
            Component::<Visualizer>::new(Visualizer { file, tree }, storage)
        })
        .title("Nebari B-Tree Explorer"),
    )
    .with_component::<Visualizer>()
    .with_canvas()
}

fn main() {
    app().run();
}

#[derive(Debug)]
struct Visualizer {
    file: TreeFile<UnversionedTreeRoot, MemoryFile>,
    tree: Arc<Mutex<Tree<UnversionedByIdIndex, ByIdStats>>>,
}

impl Behavior for Visualizer {
    type Content = Layout;
    type Event = VisualizerEvent;
    type Widgets = VisualizerWidget;

    fn build_content(
        &mut self,
        builder: <Self::Content as Content<Self>>::Builder,
        events: &EventMapper<Self>,
    ) -> StyledWidget<Layout> {
        builder
            .with(
                VisualizerWidget::Canvas,
                Canvas::new(TreeCanvas::new(self.tree.clone())),
                WidgetLayout::fill(),
            )
            .with(
                VisualizerWidget::IdField,
                Input::build()
                    .on_changed(events.map(|_| VisualizerEvent::IdChanged))
                    .finish()
                    .with(Autofocus),
                WidgetLayout::build()
                    .right(Dimension::exact(10.))
                    .top(Dimension::exact(10.))
                    .height(Dimension::exact(20.))
                    .width(Dimension::exact(100.))
                    .finish(),
            )
            .with(
                VisualizerWidget::IdButton,
                Button::build()
                    .labeled("Insert")
                    .on_clicked(events.map(|_| VisualizerEvent::IdButtonClicked))
                    .default()
                    .finish(),
                WidgetLayout::build()
                    .right(Dimension::exact(10.))
                    .top(Dimension::exact(30.))
                    .height(Dimension::exact(20.))
                    .width(Dimension::exact(100.))
                    .finish(),
            )
            .finish()
    }

    fn receive_event(
        component: &mut Component<Self>,
        event: Self::Event,
        context: &Context<Component<Self>>,
    ) {
        match event {
            VisualizerEvent::IdChanged => {}
            VisualizerEvent::IdButtonClicked => {
                let id = component
                    .map_widget(
                        &VisualizerWidget::IdField,
                        context,
                        |id_field: &Input, _context| id_field.value().parse::<u64>().ok(),
                    )
                    .flatten();
                if let Some(id) = id {
                    component
                        .behavior
                        .file
                        .push(0, Buffer::from(id.to_be_bytes()), Buffer::default())
                        .unwrap();
                    component.map_widget_mut(
                        &VisualizerWidget::IdField,
                        context,
                        |id_field: &mut Input, context| {
                            id_field.set_value((id + 1).to_string(), context)
                        },
                    );

                    let state = component.behavior.file.state.clone();
                    component
                        .behavior
                        .file
                        .execute(TreeUpdater {
                            tree: &component.behavior.tree,
                            state,
                        })
                        .unwrap();
                    component.map_widget(
                        &VisualizerWidget::Canvas,
                        context,
                        |canvas: &Canvas, context| {
                            canvas.refresh(context);
                        },
                    );
                }
            }
        }
    }
}

struct TreeUpdater<'a> {
    tree: &'a Arc<Mutex<Tree<UnversionedByIdIndex, ByIdStats>>>,
    state: State<UnversionedTreeRoot>,
}

impl<'a, File: ManagedFile> FileOp<File> for TreeUpdater<'a> {
    type Output = Result<(), nebari::Error>;

    fn execute(&mut self, file: &mut File) -> Self::Output {
        let mut tree = self.tree.lock();
        let state = self.state.read();
        tree.update(&state, file)
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
enum VisualizerWidget {
    IdField,
    IdButton,
    Canvas,
}

#[derive(Debug)]
enum VisualizerEvent {
    IdChanged,
    IdButtonClicked,
}
