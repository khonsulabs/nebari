use std::{collections::HashMap, sync::Arc};

use byteorder::{BigEndian, ByteOrder};
use gooey::{
    core::{
        figures::{Ceil, Figure, Point, Rect, Rectlike, Size},
        rgb,
        styles::Color,
        Scaled,
    },
    frontends::rasterizer::ContentArea,
    renderer::{Renderer, TextOptions},
};
use gooey_canvas::Renderable;
use nebari::{
    io::{File, ManagedFile},
    tree::{
        ActiveState, BTreeEntry, BTreeNode, ByIdStats, UnversionedByIdIndex, UnversionedTreeRoot,
    },
    AbortError, ArcBytes,
};
use parking_lot::Mutex;

const ROOT_COLOR: Color = rgb!(36, 35, 49);
const INTERIOR_COLOR: Color = rgb!(83, 62, 45);
const HIGHLIGHTED_COLOR: Color = rgb!(162, 112, 53);
const LEAF_COLOR: Color = rgb!(0, 97, 8);
const FOCUSED_COLOR: Color = rgb!(0, 141, 34);

pub struct TreeCanvas {
    tree: Arc<Mutex<Tree<UnversionedByIdIndex<()>, ByIdStats<()>>>>,
}

impl TreeCanvas {
    pub fn new(tree: Arc<Mutex<Tree<UnversionedByIdIndex<()>, ByIdStats<()>>>>) -> Self {
        Self { tree }
    }
}

#[derive(Debug)]
pub struct Tree<Index, ReducedIndex> {
    pub stack: Vec<u64>,
    pub last_row_height: Figure<f32, Scaled>,
    nodes: HashMap<u64, CanvasNode<Index, ReducedIndex>>,
    root: u64,
    focus_on: Option<ArcBytes<'static>>,
}

impl<Index, ReducedIndex> Default for Tree<Index, ReducedIndex> {
    fn default() -> Self {
        Self {
            nodes: HashMap::default(),
            root: 0,
            last_row_height: Figure::default(),
            stack: Vec::default(),
            focus_on: None,
        }
    }
}

impl Tree<UnversionedByIdIndex<()>, ByIdStats<()>> {
    pub fn update(
        &mut self,
        state: &ActiveState<UnversionedTreeRoot<()>>,
        file: &mut dyn File,
    ) -> Result<(), nebari::Error> {
        self.root = state.current_position;
        self.nodes.clear();
        self.convert_node(state.current_position, &state.root.by_id_root, file)?;

        if let Some(focus_on) = &self.focus_on {
            // Scan the tree for the node
            self.stack = vec![self.root];
            while let Some(node) = self.nodes.get(self.stack.last().unwrap()) {
                match &node.node {
                    TreeNode::Interior(children) => {
                        for child in children {
                            if focus_on <= &child.max_key {
                                self.stack.push(child.pointing_to);
                                break;
                            }
                        }
                    }
                    TreeNode::Leaf(_) => break,
                }
            }
        } else {
            for check_node in (0..self.stack.len()).rev() {
                if self.nodes.contains_key(&self.stack[check_node]) {
                    break;
                } else {
                    self.stack.remove(check_node);
                }
            }
        }
        if self.stack.is_empty() {
            self.stack.push(self.root);
        }
        Ok(())
    }

    fn convert_node(
        &mut self,
        position: u64,
        entry: &BTreeEntry<UnversionedByIdIndex<()>, ByIdStats<()>>,
        file: &mut dyn File,
    ) -> Result<(), nebari::Error> {
        let node = match &entry.node {
            BTreeNode::Leaf(children) => TreeNode::Leaf(
                children
                    .iter()
                    .map(|child| TreeLeaf {
                        key: child.key.clone(),
                        value: child.index.clone(),
                    })
                    .collect(),
            ),
            BTreeNode::Uninitialized => unreachable!(),
            BTreeNode::Interior(children) => {
                let mut converted = Vec::new();
                for child in children {
                    let pointing_to = child.position.position().expect("node had no position");
                    child
                        .position
                        .map_loaded_entry(file, None, None, None, |child, file| {
                            self.convert_node(pointing_to, child, file)
                                .map_err(AbortError::Nebari)
                        })
                        .map_err(AbortError::infallible)?;
                    converted.push(TreeInterior {
                        max_key: child.key.clone(),
                        pointing_to,
                        stats: child.stats.clone(),
                    });
                }
                TreeNode::Interior(converted)
            }
        };
        self.nodes.insert(
            position,
            CanvasNode {
                position,
                max_key: entry.max_key().clone(),
                node,
            },
        );
        Ok(())
    }
}

#[derive(Debug)]
pub struct CanvasNode<Index, ReducedIndex> {
    pub position: u64,
    pub max_key: ArcBytes<'static>,
    pub node: TreeNode<Index, ReducedIndex>,
}

#[derive(Debug)]
pub enum TreeNode<Index, ReducedIndex> {
    Interior(Vec<TreeInterior<ReducedIndex>>),
    Leaf(Vec<TreeLeaf<Index>>),
}

impl<Index, ReducedIndex> TreeNode<Index, ReducedIndex> {
    pub fn len(&self) -> usize {
        match self {
            TreeNode::Interior(children) => children.len(),
            TreeNode::Leaf(children) => children.len(),
        }
    }
}

#[derive(Debug)]
pub struct TreeInterior<ReducedIndex> {
    pub max_key: ArcBytes<'static>,
    pub pointing_to: u64,
    pub stats: ReducedIndex,
}

#[derive(Debug)]
pub struct TreeLeaf<Index> {
    pub key: ArcBytes<'static>,
    pub value: Index,
}

impl Renderable for TreeCanvas {
    fn render(
        &mut self,
        renderer: gooey_canvas::CanvasRenderer,
        content_area: &gooey::frontends::rasterizer::ContentArea,
    ) {
        let mut tree = self.tree.lock();

        let mut y = Figure::new(0.);
        let bounds = content_area.bounds().as_sized();
        let center_x = bounds.origin.x() + bounds.size.width() / 2.;
        let mut row_height = Figure::default();
        let mut previous_node_lowest_value = Option::<u64>::None;

        for (stack_index, node_position) in tree.stack.iter().enumerate() {
            let node = match tree.nodes.get(node_position) {
                Some(node) => node,
                None => break,
            };
            let max_key = BigEndian::read_u64(&node.max_key);
            let node_label = format!("Node at position {} - Max Key {}", node_position, max_key);
            let text_options = TextOptions::build()
                .text_size(16.)
                .color(Color::WHITE)
                .finish();
            let label_size = renderer.measure_text(&node_label, &text_options);
            let heading_height = label_size.height().ceil() + 10.;
            row_height = heading_height;

            renderer.fill_rect(
                &Rect::sized(
                    Point::from_figures(Figure::default(), y),
                    Size::from_figures(bounds.size.width(), heading_height),
                )
                .as_rect(),
                ROOT_COLOR,
            );
            renderer.render_text(
                &node_label,
                Point::from_figures(center_x - label_size.width / 2., y + label_size.ascent + 5.),
                &text_options,
            );

            y += heading_height;

            // Draw the children
            // To render a tree, we need to know the total width and total height.
            // The total height is easy: depth. The width can be calculated by pre-laying out the individual nodes, and measuring the full width of all the leaf nodes, including padding to space them out.
            // Once we have all the sizes, we can start rendering from the bottom up... that's the answer, just render from the bottom up.
            // Draw each row, starting from the leaves. Then, omit spacing until the depth matches
            let next_position = tree.stack.get(stack_index + 1);
            match &node.node {
                TreeNode::Interior(children) => {
                    renderer.fill_rect(
                        &Rect::sized(
                            Point::from_figures(Figure::default(), y),
                            Size::from_figures(bounds.size.width(), heading_height),
                        )
                        .as_rect(),
                        INTERIOR_COLOR,
                    );
                    let child_width = bounds.size.width() / children.len() as f32;
                    for (index, child) in children.iter().enumerate() {
                        let x = child_width * index as f32;
                        let child_max_key = BigEndian::read_u64(&child.max_key);

                        let last_max_key = if index > 0 {
                            BigEndian::read_u64(&children[index - 1].max_key).to_string()
                        } else if let Some(previous_lowest_value) = previous_node_lowest_value {
                            previous_lowest_value.to_string()
                        } else {
                            String::from("")
                        };

                        if next_position == Some(&child.pointing_to) {
                            previous_node_lowest_value = if index > 0 {
                                Some(BigEndian::read_u64(&children[index - 1].max_key))
                            } else {
                                None
                            };
                            // Highlight this one.
                            renderer.fill_rect(
                                &Rect::sized(
                                    Point::from_figures(x, y),
                                    Size::from_figures(child_width, heading_height),
                                )
                                .as_rect(),
                                HIGHLIGHTED_COLOR,
                            );
                        }

                        let label = format!(
                            "{}..={} ({})",
                            last_max_key, child_max_key, child.pointing_to
                        );
                        let label_metrics = renderer.measure_text(&label, &text_options);
                        renderer.render_text(
                            &label,
                            Point::<f32, Scaled>::from_figures(
                                x + (child_width - label_metrics.width) / 2.,
                                y + label_metrics.ascent + 5.,
                            ),
                            &text_options,
                        );
                    }
                }
                TreeNode::Leaf(children) => {
                    renderer.fill_rect(
                        &Rect::sized(
                            Point::from_figures(Figure::default(), y),
                            Size::from_figures(bounds.size.width(), heading_height),
                        )
                        .as_rect(),
                        LEAF_COLOR,
                    );
                    let child_width = (bounds.size.width() / children.len() as f32).ceil();
                    for (index, child) in children.iter().enumerate() {
                        let x = child_width * index as f32;
                        // This should be investigated and reported if repro
                        if tree.focus_on.as_ref() == Some(&child.key) {
                            // Highlight this one.
                            renderer.fill_rect(
                                &Rect::sized(
                                    Point::from_figures(x, y),
                                    Size::from_figures(child_width, heading_height),
                                )
                                .as_rect(),
                                FOCUSED_COLOR,
                            );
                        }

                        let key = BigEndian::read_u64(&child.key);
                        let label = key.to_string();
                        let label_metrics = renderer.measure_text(&label, &text_options);
                        renderer.render_text(
                            &label,
                            Point::<f32, Scaled>::from_figures(
                                x + (child_width - label_metrics.width) / 2.,
                                y + label_metrics.ascent + 5.,
                            ),
                            &text_options,
                        );
                    }
                }
            }
            y += heading_height;
        }
        tree.last_row_height = row_height;
    }

    fn mouse_down(&mut self, location: Point<f32, Scaled>, content_area: &ContentArea) -> bool {
        let mut tree = self.tree.lock();
        if tree.last_row_height.get() > 0. {
            let row = (location.y() / (tree.last_row_height)).get() as usize;
            // Each stack entry is two rows tall.
            let entry_index = row / 2;
            if row % 2 == 0 {
                return false;
            }

            if entry_index >= tree.stack.len() {
                // Past the end of the tree
                return false;
            }

            let result = if let Some(clicked_node) = tree.nodes.get(&tree.stack[entry_index]) {
                let child_width = content_area.bounds().width() / clicked_node.node.len() as f32;
                let clicked_child = location.x() / child_width;
                if clicked_child.get() > 0. {
                    let clicked_child = clicked_child.get() as usize;
                    match &clicked_node.node {
                        TreeNode::Interior(children) => {
                            Some(ClickResult::PushNode(children[clicked_child].pointing_to))
                        }
                        TreeNode::Leaf(children) => {
                            Some(ClickResult::FocusOn(children[clicked_child].key.clone()))
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            };
            match result {
                Some(ClickResult::FocusOn(key)) => {
                    tree.focus_on = Some(key);
                    true
                }
                Some(ClickResult::PushNode(new_node)) => {
                    tree.focus_on = None;
                    tree.stack.resize(entry_index + 1, 0);
                    tree.stack.push(new_node);
                    true
                }
                None => false,
            }
        } else {
            false
        }
    }
}

enum ClickResult {
    PushNode(u64),
    FocusOn(ArcBytes<'static>),
}
